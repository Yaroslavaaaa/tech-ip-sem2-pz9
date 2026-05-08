## Практическая работа №9. Вуйко Ярослава, ЭФМО-01-25
### Реализация распределённого кэша (Redis cluster). 08.05.2026



В рамках практического занятия был реализован распределённый кэш для сервиса `tasks` на базе Redis. Целью работы было ускорить чтение данных и снизить нагрузку на базу данных, а также обеспечить отказоустойчивость — при недоступности Redis сервис должен продолжать работать, получая данные напрямую из PostgreSQL.

### Структура проекта

```
.
│   go.mod
│   go.sum
│   README.md
├── .github/
│   └── workflows/
│       └── ci.yml
├── deploy/
│   └── monitoring/
│   │   ├── docker-compose.yml
│   │   └── init.sql
│   └── monitoring/
│   │   ├── docker-compose.yml
│   │   └── prometheus.yml
│   └── monitoring/
│   │   └── docker-compose.yml
│   └── tls/
│       ├── cert.pem
│       ├── docker-compose.yml
│       ├── key.pem
│       └── nginx.conf
├───docs/
│     ├───  pz17_api.md
│     └───  pz17_diagram.md
├───proto/
│    └───auth.proto
├───pkg/
│    ├───auth.pb.go
│    └───auth_grpc.pb.go
├───services/
│   ├───auth/
│   │   ├───Dockerfile
│   │   ├───go.mod
│   │   ├───go.sum
│   │   ├───cmd/
│   │   │   └───auth/
│   │   │          └─── main.go
│   │   └───internal/
│   │       ├───grpc/
│   │       │       └─── server.go
│   │       ├───handler/
│   │       │       └─── auth_handler.go
│   │       └───service/
│   │               └─── auth_servise.go
│   └───tasks/
│       ├───Dockerfile
│       │   go.mod
│       │   go.sum
│       ├───cmd/
│       │   └───tasks/
│       │           └───main.go
│       └───internal/
│           ├───cache/
│           │       └───redis.go
│           ├───client/
│           │       └───auth_client.go
│           ├───csrf/
│           │       └───middleware.go
│           ├───handler/
│           │       └───task_handler.go
│           ├───metrics/
│           │       └─── metrics.go
│           ├───repository/
│           │       ├───postgres.go
│           │       └───task_repository.go
│           └───service/
│                   └───task_service.go
└───shared/
    ├───httpx/
    │       └───client.go
    ├───logger/
    │       └───logger.go
    ├───middleware/
    │       ├───accesslog.go
    │       ├───metrics.go
    │       └───requestid.go
    └───models/
            └───models.go
```

### Ключи кэша и их формирование

Для кэширования используется один тип ключей:

| Тип данных | Формат ключа | Пример |
|------------|--------------|--------|
| Задача по ID | `tasks:task:{id}` | `tasks:task:t_a1b2c3d4` |

Ключи формируются в сервисном слое с помощью функции:

cacheKey := fmt.Sprintf("tasks:task:%s", id)

Такой формат удобен тем, что:
- Позволяет быстро найти все кэшированные задачи
- Обеспечивает уникальность для каждой сущности
- Легко инвалидируется при изменении задачи


### Реализация cache-aside

Стратегия cache-aside реализована в методе GetTaskByID сервисного слоя. Алгоритм работы:

<img width="640" height="624" alt="image" src="https://github.com/user-attachments/assets/07d35b46-8c8c-4277-aff6-a23eb42eefe1" />


### TTL и jitter

Для кэша задач установлен TTL = 2 минуты. Это значение выбрано исходя из того, что задачи в учебном приложении меняются нечасто, но при этом кэш не должен хранить устаревшие данные слишком долго.


Для предотвращения cache avalanche(ситуации, когда много ключей истекают одновременно, вызывая всплеск нагрузки на БД) добавлен случайный разброс TTL.

Реализация:
```go
func (c *RedisClient) GetTTL() time.Duration {
    if c.jitter == 0 {
        return c.ttl
    }
    jitterMs := time.Duration(rand.Int63n(int64(c.jitter)))
    return c.ttl + jitterMs
}
```

Параметры:
- Базовый TTL: 120 секунд
- Jitter: 30 секунд
- Итоговый TTL: от 120 до 150 секунд

Это гарантирует, что ключи будут истекать в разное время, равномерно распределяя нагрузку на базу данных.


### Инвалидация при обновлении (PATCH)

```
func (s *TaskService) UpdateTask(...) {
    // ... обновление в БД ...
    
    // Инвалидация кэша
    if s.redisCache != nil && s.redisCache.IsEnabled() {
        cacheKey := fmt.Sprintf("tasks:task:%s", id)
        s.redisCache.Delete(ctx, cacheKey)
    }
}
```

### Инвалидация при удалении (DELETE)

```
func (s *TaskService) DeleteTask(...) {
    // ... удаление из БД ...
    
    // Инвалидация кэша
    if s.redisCache != nil && s.redisCache.IsEnabled() {
        cacheKey := fmt.Sprintf("tasks:task:%s", id)
        s.redisCache.Delete(ctx, cacheKey)
    }
}
```

### Деградация при недоступности Redis

```go
func (c *RedisClient) Get(ctx context.Context, key string) ([]byte, error) {
    if !c.enabled {
        return nil, nil
    }
    
    ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
    defer cancel()
    
    val, err := c.client.Get(ctx, key).Bytes()
    if err == redis.Nil {
        return nil, nil
    }
    if err != nil {
        c.logger.Debug("Redis get failed (non-critical)", zap.Error(err))
        return nil, nil // игнорируем ошибку
    }
    return val, nil
}
```

### Поведение при недоступном Redis

| Операция | Поведение |
|----------|-----------|
| GET запрос | Данные читаются из PostgreSQL, ответ 200 OK |
| PATCH запрос | Данные обновляются в PostgreSQL, ответ 200 OK |
| DELETE запрос | Данные удаляются из PostgreSQL, ответ 204 No Content |
| Логи | Появляются Debug-сообщения о проблемах с Redis |
| Клиент | Не замечает проблем с кэшем |



### Конфигурация docker-compose для redis

Файл `deploy/tls/docker-compose.yml` содержит сервис Redis:

```yaml
redis:
  image: redis:7-alpine
  container_name: redis-cache
  ports:
    - "6379:6379"
  volumes:
    - redis_data:/data
  command: redis-server --appendonly yes
  networks:
    - tls-network
  healthcheck:
    test: ["CMD", "redis-cli", "ping"]
    interval: 5s
    timeout: 3s
    retries: 5
```

### Переменные окружения для tasks сервиса

```
tasks:
  environment:
    - REDIS_ADDR=redis:6379
    - REDIS_PASSWORD=
    - REDIS_DB=0
    - CACHE_TTL_SECONDS=120
    - CACHE_TTL_JITTER_SECONDS=30
```

### Запуск

```
cd deploy/tls
docker-compose up --build -d
```

### Проверка работы Redis

```
# Проверка статуса
docker ps | grep redis

# Подключение к Redis CLI
docker exec -it redis-cache redis-cli

# Проверка ключей
KEYS *
```


## Примеры запросов и результатов

### Создание задачи

<img width="1383" height="695" alt="2026-05-08_18-14-18" src="https://github.com/user-attachments/assets/1a5a303d-1cfc-47f4-99fc-63d61938c4e0" />



### Первый запрос (Cache Miss). Запрос выполнилсяза 62 ms

<img width="1378" height="797" alt="2026-05-08_18-15-23" src="https://github.com/user-attachments/assets/79a44c99-9002-4ef4-8701-18d61c4cef98" />

### Второй запрос (Cache Hit). Запрос выполнился за 6 ms

<img width="1372" height="802" alt="2026-05-08_18-15-44" src="https://github.com/user-attachments/assets/dd368db2-7c39-471b-b0df-ecebb8a6c49a" />


### Обновление задачи (инвалидация кэша)

<img width="1360" height="807" alt="2026-05-08_18-21-38" src="https://github.com/user-attachments/assets/1111212b-0379-4f29-a80c-54743dfcafa7" />


### Симуляция недоступности Redis

```bash
docker stop redis-cache

```
Запрос выполнен, но данные получены из БД. Время выполнения 1 секунда
<img width="1376" height="797" alt="2026-05-08_18-38-28" src="https://github.com/user-attachments/assets/fd212464-8f7c-46dc-8a6b-8f17dc22b57b" />

### Вывод

В результате практического занятия был успешно реализован распределённый кэш для сервиса `tasks` на базе Redis. Реализованы:

1. Cache-aside — ленивая загрузка данных в кэш
2. TTL с jitter — 120 сек + случайный разброс до 30 сек
3. Инвалидация кэша — при обновлении и удалении задач
4. Деградация — сервис работает даже при недоступном Redis
5. Короткие таймауты — операции Redis не блокируют запросы

### Контрольные вопросы

1. Что такое cache-aside и почему он часто используется?
Cache-aside (lazy loading) — стратегия, при которой приложение сначала проверяет кэш, и только при промахе обращается к БД. Она проста в реализации, не требует предварительного заполнения кэша и хорошо работает для read-heavy приложений.

2. Зачем нужен TTL?
TTL (Time To Live) — время жизни данных в кэше. Он нужен, чтобы кэш не хранил устаревшие данные бесконечно и автоматически обновлялся. Это также помогает управлять объёмом памяти, занимаемой кэшем.

3. Что такое cache avalanche и как jitter помогает?
Cache avalanche — ситуация, когда множество ключей истекают одновременно, вызывая резкий всплеск запросов в БД. Jitter (случайный разброс TTL) решает эту проблему, заставляя ключи истекать в разное время, равномерно распределяя нагрузку.

4. Почему Redis не должен быть "источником истины"?
Redis — это volatile-хранилище (данные могут быть потеряны при перезапуске, вытеснены при нехватке памяти). Источником истины должна быть только постоянная БД (PostgreSQL, MySQL и т.д.). Redis используется только для ускорения чтения.

5. Как правильно вести себя сервису при падении Redis?
Сервис должен:
- Логировать ошибку (как WARN/DEBUG, не как ERROR)
- Автоматически переключаться на прямое чтение из БД
- Не падать и не возвращать ошибку клиенту
- Не увеличивать время ответа более чем на допустимый порог

