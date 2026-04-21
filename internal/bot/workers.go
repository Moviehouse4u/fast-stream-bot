package bot

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/biisal/fast-stream-bot/config"
	"github.com/biisal/fast-stream-bot/internal/service/user"
	"github.com/gotd/td/session"
	"github.com/gotd/td/session/tdsession"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
	"github.com/redis/go-redis/v9"
)

const (
	WorkingTimerSec int = 30
)

type Worker struct {
	Bots            []*Bot
	mut             sync.Mutex
	RunningBotIndex int
	Timer           time.Time
}

func initWorker() *Worker {
	return &Worker{
		Timer: time.Now(),
	}
}

func startClient(worker *Worker, botToken string, cfg *config.Config, workerNum int,
	wg *sync.WaitGroup, userService user.Service, redisClient *redis.Client,
) {
	done := false
	defer func() {
		if !done {
			wg.Done()
		}
	}()
	ctx := context.Background()
	dispatcher := tg.NewUpdateDispatcher()

	sessionKey := fmt.Sprintf("tg_session:%d", workerNum)
	storage := tdsession.NewRedis(redisClient, sessionKey)

	client := telegram.NewClient(cfg.APP_KEY, cfg.APP_HASH, telegram.Options{
		UpdateHandler:  dispatcher,
		SessionStorage: storage,
	})

	isDefault := workerNum == 0
	bot := NewBot(ctx, cfg, client, &dispatcher, userService, isDefault)
	if isDefault {
		bot.SetUpOnMessage()
	}
	if err := client.Run(ctx, func(ctx context.Context) error {
		if _, err := client.Auth().Bot(ctx, botToken); err != nil {
			return err
		}
		worker.mut.Lock()
		worker.Bots = append(worker.Bots, bot)
		worker.mut.Unlock()
		self, err := client.Self(ctx)
		if err != nil {
			return err
		}
		if self.Bot {
			bot.BotUserName = self.Username
		}
		slog.Info("Bot started", "bot_username", bot.BotUserName)
		if _, err := bot.Sender.To(&tg.InputPeerUser{UserID: cfg.ADMIN_ID}).Text(ctx, "Bot is running"); err != nil {
			slog.Error("Failed to send status message to admin", "error", err)
		}
		done = true
		wg.Done()
		<-ctx.Done()
		return nil
	}); err != nil {
		slog.Error("Failed to start bot", "error", err)
	}
}

func StartWorkers(cfg *config.Config, userService user.Service, redisClient *redis.Client) *Worker {
	worker := initWorker()
	var wg sync.WaitGroup
	for i, botToken := range cfg.BOT_TOKENS {
		wg.Add(1)
		go startClient(worker, botToken, cfg, i, &wg, userService, redisClient)
	}
	slog.Debug("Waiting for bot workers to start")
	wg.Wait()
	slog.Info("Bot workers started")
	return worker
}

func (w *Worker) HireFreeWorker() (*Bot, error) {
	w.mut.Lock()
	defer w.mut.Unlock()
	totalBots := len(w.Bots)
	if totalBots == 0 {
		return nil, fmt.Errorf("no bots available in worker pool")
	}
	selected := w.Bots[w.RunningBotIndex]
	if time.Since(w.Timer) < time.Duration(WorkingTimerSec)*time.Second {
		selected.WorkingPressure++
		return selected, nil
	}
	botIdx := 0
	minPressure := w.Bots[botIdx].WorkingPressure
	for i, bot := range w.Bots {
		if bot.WorkingPressure < minPressure {
			minPressure = bot.WorkingPressure
			botIdx = i
		}
		if minPressure <= 0 {
			break
		}
	}
	w.Timer = time.Now()
	selected = w.Bots[botIdx]
	selected.WorkingPressure++
	return selected, nil
}

func (w *Worker) ReleaseWorker(bot *Bot) {
	w.mut.Lock()
	defer w.mut.Unlock()
	if bot.WorkingPressure > 0 {
		bot.WorkingPressure--
	}
}
