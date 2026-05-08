package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"tasks-service/internal/cache"
	"tasks-service/internal/client"
	"tasks-service/internal/repository"
	"tech-ip-sem2/shared/logger"
	"tech-ip-sem2/shared/models"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

type TaskService struct {
	authClient *client.AuthClient
	repo       *repository.TaskRepository
	redisCache *cache.RedisClient
	logger     *zap.Logger
}

func NewTaskService(
	authClient *client.AuthClient,
	repo *repository.TaskRepository,
	redisCache *cache.RedisClient,
	logger *zap.Logger,
) *TaskService {
	return &TaskService{
		authClient: authClient,
		repo:       repo,
		redisCache: redisCache,
		logger:     logger.With(zap.String("component", "service")),
	}
}

func (s *TaskService) Create(ctx context.Context, token string, title, description, dueDate string) (models.Task, error) {
	requestID, _ := ctx.Value(logger.RequestIDKey{}).(string)

	log := s.logger.With(
		zap.String("request_id", requestID),
		zap.String("operation", "create"),
	)

	username, err := s.authClient.VerifyToken(ctx, token)
	if err != nil {
		return models.Task{}, fmt.Errorf("auth failed: %w", err)
	}

	task := models.Task{
		ID:          "t_" + uuid.New().String()[:8],
		Title:       title,
		Description: description,
		DueDate:     dueDate,
		Done:        false,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	if err := s.repo.Create(ctx, task); err != nil {
		return models.Task{}, err
	}

	log.Info("Task created",
		zap.String("task_id", task.ID),
		zap.String("username", username),
	)

	return task, nil
}

func (s *TaskService) GetAll(ctx context.Context, token string) ([]models.Task, error) {
	_, err := s.authClient.VerifyToken(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("auth failed: %w", err)
	}

	return s.repo.GetAll(ctx)
}

// GetByID возвращает задачу по ID с поддержкой кэширования (cache-aside)
func (s *TaskService) GetByID(ctx context.Context, token, id string) (models.Task, error) {
	requestID, _ := ctx.Value(logger.RequestIDKey{}).(string)

	log := s.logger.With(
		zap.String("request_id", requestID),
		zap.String("operation", "get_by_id"),
		zap.String("task_id", id),
	)

	_, err := s.authClient.VerifyToken(ctx, token)
	if err != nil {
		return models.Task{}, fmt.Errorf("auth failed: %w", err)
	}

	cacheKey := fmt.Sprintf("tasks:task:%s", id)

	if s.redisCache != nil {
		cachedData, err := s.redisCache.Get(ctx, cacheKey)
		if err == nil && cachedData != nil {
			var task models.Task
			if err := json.Unmarshal(cachedData, &task); err == nil {
				log.Debug("Cache hit")
				return task, nil
			}
			log.Warn("Failed to unmarshal cached task", zap.Error(err))
		} else if err != nil {
			log.Warn("Cache read failed, falling back to DB", zap.Error(err))
		} else {
			log.Debug("Cache miss")
		}
	}

	task, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return models.Task{}, fmt.Errorf("task not found: %w", err)
	}

	if s.redisCache != nil {
		ttl := s.redisCache.GetTTL()
		if err := s.redisCache.Set(ctx, cacheKey, task, ttl); err != nil {
			log.Warn("Failed to cache task", zap.Error(err))
		}
	}

	log.Debug("Cache miss - loaded from DB")
	return task, nil
}

// Update обновляет задачу и инвалидирует кэш
func (s *TaskService) Update(ctx context.Context, token, id string, title *string, done *bool) (models.Task, error) {
	requestID, _ := ctx.Value(logger.RequestIDKey{}).(string)

	log := s.logger.With(
		zap.String("request_id", requestID),
		zap.String("operation", "update"),
		zap.String("task_id", id),
	)

	_, err := s.authClient.VerifyToken(ctx, token)
	if err != nil {
		return models.Task{}, fmt.Errorf("auth failed: %w", err)
	}

	task, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return models.Task{}, fmt.Errorf("task not found")
	}

	if title != nil {
		task.Title = *title
	}
	if done != nil {
		task.Done = *done
	}

	task.UpdatedAt = time.Now()

	if err := s.repo.Update(ctx, task); err != nil {
		return models.Task{}, err
	}

	// Инвалидация кэша — удаляем ключ задачи
	if s.redisCache != nil {
		cacheKey := fmt.Sprintf("tasks:task:%s", id)
		if err := s.redisCache.Delete(ctx, cacheKey); err != nil {
			log.Warn("Failed to invalidate cache", zap.Error(err))
		}
	}

	log.Info("Task updated, cache invalidated")
	return task, nil
}

// Delete удаляет задачу и инвалидирует кэш
func (s *TaskService) Delete(ctx context.Context, token, id string) error {
	requestID, _ := ctx.Value(logger.RequestIDKey{}).(string)

	log := s.logger.With(
		zap.String("request_id", requestID),
		zap.String("operation", "delete"),
		zap.String("task_id", id),
	)

	_, err := s.authClient.VerifyToken(ctx, token)
	if err != nil {
		return fmt.Errorf("auth failed: %w", err)
	}

	if err := s.repo.Delete(ctx, id); err != nil {
		return fmt.Errorf("task not found")
	}

	// Инвалидация кэша — удаляем ключ задачи
	if s.redisCache != nil {
		cacheKey := fmt.Sprintf("tasks:task:%s", id)
		if err := s.redisCache.Delete(ctx, cacheKey); err != nil {
			log.Warn("Failed to invalidate cache", zap.Error(err))
		}
	}

	log.Info("Task deleted, cache invalidated")
	return nil
}

func (s *TaskService) SearchByTitle(ctx context.Context, token, title string) ([]models.Task, error) {
	requestID, _ := ctx.Value(logger.RequestIDKey{}).(string)

	log := s.logger.With(
		zap.String("request_id", requestID),
		zap.String("operation", "search"),
	)

	_, err := s.authClient.VerifyToken(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("auth failed: %w", err)
	}

	tasks, err := s.repo.SearchByTitle(ctx, title)
	if err != nil {
		log.Error("Failed to search tasks", zap.Error(err))
		return nil, fmt.Errorf("failed to search tasks: %w", err)
	}

	log.Info("Search completed",
		zap.Int("count", len(tasks)),
		zap.String("search_term", title))

	return tasks, nil
}
