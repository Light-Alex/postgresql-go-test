package main

import (
	"context"
	"fmt"
	"log"

	"gorm.io/gorm"
)

type BaseRepository[T any] struct {
	db *gorm.DB
}

// NewBaseRepository 创建基础仓库
func NewBaseRepository[T any](db *gorm.DB) *BaseRepository[T] {
	return &BaseRepository[T]{db: db}
}

// CreateTable 创建表
func (r *BaseRepository[T]) CreateTable(entity *T) error {
	if err := r.db.AutoMigrate(entity); err != nil {
		return fmt.Errorf("表 %T 自动迁移失败: %w", entity, err)
	}
	log.Printf("表 %T 创建成功!", entity)
	return nil
}

// Create 创建实体
func (r *BaseRepository[T]) Create(ctx context.Context, entity *T) error {
	return r.db.WithContext(ctx).Create(entity).Error
}

// BatchCreate 批量创建实体
func (r *BaseRepository[T]) BatchCreate(ctx context.Context, entities []*T) error {
	return r.db.WithContext(ctx).Create(entities).Error
}

// GetByID 根据ID查询实体
func (r *BaseRepository[T]) GetByID(ctx context.Context, id uint) (*T, error) {
	var entity T
	err := r.db.WithContext(ctx).First(&entity, id).Error
	if err != nil {
		return nil, err
	}
	return &entity, nil
}

// Update 更新实体
func (r *BaseRepository[T]) Update(ctx context.Context, entity *T) error {
	return r.db.WithContext(ctx).Save(entity).Error
}

// Delete 删除实体
func (r *BaseRepository[T]) Delete(ctx context.Context, id uint) error {
	// 软删除
	return r.db.WithContext(ctx).Delete(new(T), id).Error

	// 硬删除（谨慎使用）
	// return r.db.WithContext(ctx).Unscoped().Delete(new(T), id).Error
}

// ListAll 查询所有实体
func (r *BaseRepository[T]) ListAll(ctx context.Context) ([]*T, error) {
	var entities []*T
	err := r.db.WithContext(ctx).Find(&entities).Error
	return entities, err
}

// List 根据offset和limit查询实体列表
func (r *BaseRepository[T]) List(ctx context.Context, offset, limit int) ([]*T, int64, error) {
	var entities []*T
	var total int64

	if total, err := r.Count(ctx); err != nil {
		return nil, total, err
	}

	err := r.db.WithContext(ctx).Offset(offset).Limit(limit).Find(&entities).Error
	return entities, total, err
}

// Count 查询实体总数
func (r *BaseRepository[T]) Count(ctx context.Context) (int64, error) {
	var count int64
	err := r.db.WithContext(ctx).Model(new(T)).Count(&count).Error
	return count, err
}

// GetDB 获取原始的gorm.DB实例
func (r *BaseRepository[T]) GetDB() *gorm.DB {
	return r.db
}
