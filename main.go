package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mymmrac/telego"
	"github.com/mymmrac/telego/telegoapi"
	th "github.com/mymmrac/telego/telegohandler"
	tu "github.com/mymmrac/telego/telegoutil"

	_ "turso.tech/database/tursogo"
	turso "turso.tech/database/tursogo"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	botToken := BotToken

	syncDb, err := turso.NewTursoSyncDb(ctx, turso.TursoSyncDbConfig{
		Path:      "pplphase.db",
		RemoteUrl: TursoUrl,
		AuthToken: TursoToken,
	})
	if err != nil {
		log.Fatal("cant start DB")
	}

	db, err := syncDb.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	bot, err := telego.NewBot(botToken, telego.WithAPICaller(&telegoapi.RetryCaller{Caller: telegoapi.DefaultFastHTTPCaller,
		MaxAttempts:  5,
		ExponentBase: 2,
		StartDelay:   100 * time.Millisecond,
		MaxDelay:     10 * time.Second,
		RateLimit:    telegoapi.RetryRateLimitWait}))
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	botUser, err := bot.GetMe(ctx)
	if err != nil {
		log.Fatal("Error: ", err)
	}

	log.Printf("Bot user: %+v\n", botUser)

	updates, err := bot.UpdatesViaLongPolling(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	phaseCh := make(chan *Phase, 1)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		cancel()
	}()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); timer(ctx, phaseCh) }()
	go func() { defer wg.Done(); broadcastWorker(ctx, bot, db, phaseCh) }()
	bh, err := th.NewBotHandler(bot, updates)
	if err != nil {
		log.Fatal("cant create Bot Handler (bh): ", err)
	}

	defer func() { _ = bh.Stop() }()

	HandleCommands(bot, bh, db, syncDb)

	log.Println("bot is booting up")
	err = bh.Start()
	if err != nil {
		log.Print(err)
	}
	cancel()
	wg.Wait()
}

func HandleCommands(bot *telego.Bot, bh *th.BotHandler, db *sql.DB, syncDb *turso.TursoSyncDb) {
	bh.Handle(func(ctx *th.Context, update telego.Update) error {
		if update.Message == nil {
			return nil
		}

		message := *update.Message
		chatID := message.Chat.ID

		_, err := db.ExecContext(ctx, "INSERT OR IGNORE INTO users (id) VALUES (?)", chatID)
		if err != nil {
			log.Printf("insert user %d failed: %v", chatID, err)
			bot.SendMessage(ctx, tu.Message(tu.ID(chatID), "Что-то пошло не так, попробуйте ещё раз"))
			return nil
		}
		_, err = bot.SendMessage(ctx, tu.Message(tu.ID(update.Message.Chat.ID), "Наблюдать фазы Pepeland 11 бесплатно без СМС и регистраций. Раз в некоторое время идет проверка фазы. Если фаза сменилась, то вам придет сообщение\n\nБыло написано за пару часов. Доступ предоставляется \"Как есть\", идеальная работа не гарантирована"))
		if err != nil {
			log.Printf("send to %d failed: %v", chatID, err)
			return nil
		}

		log.Printf("new /start from %d", chatID)

		body := fetchApi()
		var phase Phase
		var phase_msg string
		if body != nil && parsePhase(body, &phase) == nil && phase.Current_phase != "" {
			phase_msg = fmt.Sprintf("Текущая фаза - %s\nКэшированная фаза - %s", phase.Current_phase, phase.Cached_phase)
		} else {

			phase_msg = "Конкретно сейчас не удалось получить текущую фазу. Но в рассылку вас записал - при смене фазы напишу"

		}

		if err := syncDb.Push(ctx); err != nil {
			log.Printf("turso push failed: %v", err)
		}

		_, err = bot.SendMessage(ctx, tu.Message(tu.ID(chatID), phase_msg))
		if err != nil {
			log.Printf("send to %d failed: %v", chatID, err)
			return nil
		}
		return nil
	}, th.CommandEqual("start"))

}

type Phase struct {
	Status        int    `json:"status"`
	Current_phase string `json:"current_phase"`
	Cached_phase  string `json:"cached_phase"`
}

func timer(ctx context.Context, phaseCh chan *Phase) {
	var lastPhase string
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	if body := fetchApi(); body != nil {
		var p Phase
		if parsePhase(body, &p) == nil {
			lastPhase = p.Current_phase
		}
	}

	for true {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		body := fetchApi()
		if body == nil {
			continue
		}
		var phase Phase
		if err := parsePhase(body, &phase); err != nil {
			continue
		}

		if phase.Current_phase == "" {
			log.Print("empty current_phase, skipping")
			continue
		}

		if phase.Current_phase != lastPhase {
			lastPhase = phase.Current_phase
			select {
			case phaseCh <- &phase:
			default:
			}
		}

	}
}
func broadcastWorker(ctx context.Context, bot *telego.Bot, db *sql.DB, phaseCh chan *Phase) {
	for {
		select {
		case <-ctx.Done():
			return
		case p := <-phaseCh:
			broadcastToEveryone(ctx, bot, db, p)
		}
	}
}
func broadcastToEveryone(ctx context.Context, bot *telego.Bot, db *sql.DB, phase *Phase) {
	var ids []int64
	rows, err := db.QueryContext(ctx, "select id from users")
	if err != nil {
		log.Print(err)
		return
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			log.Print(err)
			continue
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		log.Printf("users iteration failed: %v", err)
	}
	rows.Close()
	limiter := time.NewTicker(50 * time.Millisecond)
	defer limiter.Stop()

	phase_msg := fmt.Sprintf("Текущая фаза - %s\nКэшированная фаза - %s", phase.Current_phase, phase.Cached_phase)

	log.Printf("broadcast to %d users, phase=%s", len(ids), phase.Current_phase)

	for _, id := range ids {
		select {
		case <-ctx.Done():
			return
		case <-limiter.C:
		}

		if _, err := bot.SendMessage(ctx, tu.Message(tu.ID(id), phase_msg)); err != nil {
			log.Printf("send to %d failed: %v", id, err)
		}
	}
}

var httpClient = &http.Client{Timeout: 5 * time.Second}

func fetchApi() []byte {
	resp, err := httpClient.Get("http://127.0.0.1:1111/ppl_phase")
	if err != nil {
		log.Println("cant fetch api.snrkostik.dev/ppl_phase: ", err)
		return nil
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Print(err)
		return nil
	}
	return body
}
func parsePhase(body []byte, phase *Phase) error {
	err := json.Unmarshal(body, phase)
	if err != nil {
		log.Print(err)
		return err
	}
	return nil
}
