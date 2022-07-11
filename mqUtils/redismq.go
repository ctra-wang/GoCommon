package mqmodule

/**
这里日志选用的是go-zero的日志功能，如果开发者也使用此框架可以轻松接入 EFS 或者 EKS
*/
import (
	"context"
	"github.com/hibiken/asynq"
	"github.com/zeromicro/go-zero/core/logx"
	"time"
)

type RedisConfig struct {
	Channel *asynq.Client
}

func RedisMQConnect(RedisPassword string, RedisAddr string, RedisDb int) *asynq.Client {
	r := &asynq.RedisClientOpt{
		Addr:     RedisAddr,
		DB:       RedisDb,
		Password: RedisPassword,
	}
	client := asynq.NewClient(r)
	//defer client.Close()
	logx.Info("redismq connected success")
	return client
}

// RedisPushrc 发送消息队列核心方法
func (r *RedisConfig) RedisPushrc(ctx context.Context, queueName string, message []byte) error {

	// 根据传入的业务名称获取queue名称

	// 设置 一个消息队列名
	option1 := asynq.Queue(queueName)
	// 设置 最大重试次数
	option2 := asynq.MaxRetry(5)
	// 设置 超时时间
	option3 := asynq.Timeout(12 * time.Hour)

	t1 := asynq.NewTask(
		queueName, //  type 保持与queueName一致
		message,
	)
	// 10秒超时，最多重试3次，20秒后过期
	//info, err := r.Channel.Enqueue(t1, asynq.MaxRetry(3), asynq.Timeout(10*time.Second), asynq.Deadline(time.Now().Add(20*time.Second)))

	// 推送消息 创建queue的task
	info, err := r.Channel.Enqueue(t1, option1, option2, option3)
	if err != nil {
		logx.WithContext(ctx).Errorf("could not enqueue task: %v", err)
		return err
	}
	logx.WithContext(ctx).Infof("enqueued task: id=%s queue=%s", info.ID, info.Queue)

	return nil
}
