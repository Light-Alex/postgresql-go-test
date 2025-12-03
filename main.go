package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// User 用户模型
type User struct {
	ID        uint           `gorm:"primaryKey" example:"1"`
	Name      string         `gorm:"size:100;not null" validate:"required,max=20" example:"john_doe"`
	Email     string         `gorm:"size:100;uniqueIndex;not null" validate:"required,email" example:"john@example.com"`
	Age       int            `gorm:"not null" validate:"required,min=0,max=120" example:"30"`
	CreatedAt time.Time      `example:"2023-01-01T00:00:00Z"`
	UpdatedAt time.Time      `example:"2023-01-01T00:00:00Z"`
	DeletedAt gorm.DeletedAt `gorm:"index"`
}

func (User) TableName() string {
	// return "users"
	return "postgresql_test.users" // 指定schema为app_schema；PostgreSQL格式: schema.table_name
}

func (u *User) BeforeCreate(tx *gorm.DB) error {
	u.CreatedAt = time.Now()
	u.UpdatedAt = time.Now()
	return nil
}

func (u *User) BeforeUpdate(tx *gorm.DB) error {
	u.UpdatedAt = time.Now()
	return nil
}

type PostgresConfig struct {
	Host         string
	Port         int
	User         string
	Password     string
	DBName       string
	SSLMode      string
	MaxIdleConns int
	MaxOpenConns int
	MaxLifetime  int
	LogLevel     string
}

// 全局数据库连接
var DB *gorm.DB

// NewPostgresDB 初始化数据库连接
func NewPostgresDB(cfg *PostgresConfig) (*gorm.DB, error) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		return nil, fmt.Errorf("加载时区失败: %w", err)
	}
	time.Local = loc

	// PostgreSQL 17 连接字符串
	// 请根据您的实际配置修改以下参数
	dsn := fmt.Sprintf("host=%s user=%s password=%s dbname=%s port=%s sslmode=%s TimeZone=Asia/Shanghai",
		cfg.Host, cfg.User, cfg.Password, cfg.DBName, fmt.Sprintf("%d", cfg.Port), cfg.SSLMode)

	var logLevel logger.LogLevel
	switch cfg.LogLevel {
	case "silent":
		logLevel = logger.Silent
	case "error":
		logLevel = logger.Error
	case "warn":
		logLevel = logger.Warn
	case "info":
		logLevel = logger.Info
	default:
		logLevel = logger.Info
	}

	// var err error
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logLevel),
		NowFunc: func() time.Time {
			return time.Now().Local()
		},
	})
	if err != nil {
		return nil, fmt.Errorf("连接数据库失败: %v", err)
	}

	// 获取SQL数据库连接实例
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get database instance: %w", err)
	}

	// 设置连接池参数
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	if cfg.MaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(time.Duration(cfg.MaxLifetime) * time.Second)
	}

	if err := sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}
	log.Println("成功连接到PostgreSQL数据库!")

	DB = db

	return db, nil
}

func Close() error {
	if DB != nil {
		sqlDB, err := DB.DB()
		if err != nil {
			return err
		}
		return sqlDB.Close()
	}
	return nil
}

func GetDB() *gorm.DB {
	return DB
}

var _ UserRepository = (*userRepository)(nil)

type UserRepository interface {
	CreateTable(user *User) error
	Create(ctx context.Context, user *User) error
	BatchCreate(ctx context.Context, users []*User) error
	GetByID(ctx context.Context, id uint) (*User, error)
	Update(ctx context.Context, user *User) error
	Delete(ctx context.Context, id uint) error
	ListAll(ctx context.Context) ([]*User, error)
	List(ctx context.Context, offset, limit int) ([]*User, int64, error)
	Count(ctx context.Context) (int64, error)
	GetUserByAge(ctx context.Context, minAge int) ([]*User, error)
}

type userRepository struct {
	*BaseRepository[User]
}

// NewUserRepository 创建用户仓库
func NewUserRepository(db *gorm.DB) UserRepository {
	return &userRepository{
		BaseRepository: NewBaseRepository[User](db),
	}
}

// getUsersByAge 根据年龄查询用户
func (r *userRepository) GetUserByAge(ctx context.Context, minAge int) ([]*User, error) {
	var users []*User
	err := r.db.WithContext(ctx).Where("age > ?", minAge).Find(&users).Error
	if err != nil {
		return nil, fmt.Errorf("根据年龄查询用户失败: %w", err)
	}
	return users, nil
}

func main() {
	log.Println("=== GORM PostgreSQL CRUD 操作演示 ===")

	ctx := context.Background()

	// 1. 初始化数据库连接
	db, err := NewPostgresDB(&PostgresConfig{
		Host:         "192.168.140.128",
		Port:         5432,
		User:         "postgres",
		Password:     "postgres",
		DBName:       "gin_app",
		SSLMode:      "disable",
		MaxIdleConns: 10,
		MaxOpenConns: 100,
		MaxLifetime:  60,
		LogLevel:     "info",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer Close()

	// 2. 创建user仓库示例
	userRepo := NewUserRepository(db)

	// 3. 创建表结构
	if err := userRepo.CreateTable(&User{}); err != nil {
		log.Fatal(err)
	}

	// 4. 创建用户操作
	log.Println("\n=== 创建用户操作 ===")
	// 创建用户
	user := &User{
		Name:  "张三",
		Email: "zhangsan@example.com",
		Age:   25,
	}
	if err := userRepo.Create(ctx, user); err != nil {
		log.Fatal(err)
	}
	log.Printf("成功创建用户: ID=%d, 姓名=%s, 年龄=%d", user.ID, user.Name, user.Age)

	user = &User{
		Name:  "李四",
		Email: "lisi@example.com",
		Age:   30,
	}
	if err := userRepo.Create(ctx, user); err != nil {
		log.Fatal(err)
	}
	log.Printf("成功创建用户: ID=%d, 姓名=%s, 年龄=%d", user.ID, user.Name, user.Age)

	// 批量创建用户
	batchUsers := []*User{
		{Name: "王五", Email: "wangwu@example.com", Age: 28},
		{Name: "赵六", Email: "zhaoliu@example.com", Age: 35},
	}
	if err := userRepo.BatchCreate(ctx, batchUsers); err != nil {
		log.Fatal(err)
	}
	log.Printf("成功批量创建 %d 个用户", len(batchUsers))

	// 5. 查询操作
	log.Println("\n=== 查询操作 ===")

	// 查询单个用户
	user, err = userRepo.GetByID(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("成功查询用户: ID=%d, 姓名=%s, 年龄=%d", user.ID, user.Name, user.Age)

	// 查询所有用户
	users, err := userRepo.ListAll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("成功查询 %d 个用户", len(users))

	// 条件查询
	// 查询年龄大于26岁的用户
	users, err = userRepo.GetUserByAge(ctx, 26)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("成功查询 %d 个年龄大于26岁的用户", len(users))

	// 6. 更新操作
	log.Println("\n=== 更新操作 ===")

	// 更新用户年龄
	if err := userRepo.Update(ctx, &User{ID: 1, Age: 26}); err != nil {
		log.Fatal(err)
	}

	// 更新用户信息
	if err := userRepo.Update(ctx, &User{ID: 1, Name: "张三丰", Age: 27}); err != nil {
		log.Fatal(err)
	}

	// 验证更新结果
	updatedUser, err := userRepo.GetByID(ctx, 1)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("更新后验证: 姓名=%s, 年龄=%d\n", updatedUser.Name, updatedUser.Age)

	// 7. 删除操作
	log.Println("\n=== 删除操作 ===")

	// 获取删除前的用户数量
	countBefore, _ := userRepo.Count(ctx)

	// 删除用户
	if err := userRepo.Delete(ctx, 1); err != nil {
		log.Fatal(err)
	}

	// 获取删除后的用户数量
	countAfter, _ := userRepo.Count(ctx)
	log.Printf("删除前后用户数量变化: %d -> %d\n", countBefore, countAfter)

	// 8. 最终查询验证
	log.Println("\n=== 最终结果验证 ===")
	users, err = userRepo.ListAll(ctx)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("最终查询到 %d 个用户\n", len(users))

	log.Println("\n=== CRUD操作演示完成 ===")
}
