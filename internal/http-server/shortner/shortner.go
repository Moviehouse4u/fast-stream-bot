package shortner

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/biisal/fast-stream-bot/config"
	"github.com/biisal/fast-stream-bot/internal/redis"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

type Shortner interface {
	CheckJWTFromCookie(r *http.Request) bool
	SetJWTCookie(w http.ResponseWriter) error
	SetUUID(r *http.Request) string
	VerifyUUID(r *http.Request) bool
	CreateShortnerLink(urlToShort string) string
	RemoveUUID(r *http.Request)
}

type svc struct {
	JWTExpireTime  time.Duration
	UUIDExpireTime time.Duration
	Secret         []byte
	redis          redis.RedisService
	ShortnerURL    string
	ShortnerApi    string
	cfg            config.Config
}

func NewShortner(expireTime, UUIDExpireTime time.Duration, secret []byte,
	redis redis.RedisService, shortnerURL, shortnerApi string, cfg config.Config) Shortner {
	return &svc{
		expireTime,
		UUIDExpireTime,
		secret,
		redis,
		shortnerURL,
		shortnerApi,
		cfg,
	}
}

func (s *svc) CheckJWTFromCookie(r *http.Request) bool {
	cookie, err := r.Cookie("token")
	if err != nil {
		slog.Error("Failed to get cookie", "error", err)
		return false
	}

	token, err := jwt.ParseWithClaims(cookie.Value, &jwt.RegisteredClaims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, errors.New("unexpected signing method")
		}
		return s.Secret, nil
	})
	if err != nil || !token.Valid {
		slog.Error("Failed to parse token", "error", err)
		return false
	}

	return true
}

func (s *svc) SetJWTCookie(w http.ResponseWriter) error {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"exp": time.Now().Add(s.JWTExpireTime).Unix(),
	})
	tokenString, err := token.SignedString(s.Secret)
	if err != nil {
		slog.Error("Failed to sign token", "error", err)
		return err
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "token",
		Value:    tokenString,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.cfg.ENVIRONMENT == config.ENVIRONMENT_PROD,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   int(s.JWTExpireTime.Seconds()),
	})
	return nil
}

func (s *svc) VerifyUUID(r *http.Request) bool {
	id := r.URL.Query().Get("uuid")
	if id == "" {
		return false
	}
	key := fmt.Sprintf("uuid:%s", id)
	val := s.redis.Get(r.Context(), key)
	return len(val) > 0
}
func (s *svc) SetUUID(r *http.Request) string {
	id := fmt.Sprintf("%x", uuid.New())[:8]
	key := fmt.Sprintf("uuid:%s", id)
	s.redis.Set(r.Context(), key, "1", s.UUIDExpireTime)
	return id
}

func (s *svc) RemoveUUID(r *http.Request) {
	id := r.URL.Query().Get("uuid")
	if id == "" {
		return
	}
	key := fmt.Sprintf("uuid:%s", id)
	s.redis.Del(r.Context(), key)
}

var client = &http.Client{
	Timeout: 10 * time.Second,
}

type ShortnerResponse struct {
	Status       string `json:"status"`
	ShortenedUrl string `json:"shortenedUrl"`
}

func (s *svc) CreateShortnerLink(urlToShort string) string {
	encoded := url.QueryEscape(urlToShort)
	fullUrl := fmt.Sprintf("%s/api?api=%s&url=%s", s.ShortnerURL, s.ShortnerApi, encoded)
	slog.Info("Creating shortner link", "url", fullUrl)

	resp, err := client.Get(fullUrl)
	if err != nil {
		slog.Error("Failed to get shortner link", "error", err)
		return ""
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Warn("Failed to close response body", "error", err)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		slog.Error("Failed to get shortner link", "error", resp.Status)
		return ""
	}
	var shortnerResponse ShortnerResponse
	if err := json.NewDecoder(resp.Body).Decode(&shortnerResponse); err != nil {
		slog.Error("Failed to decode shortner response", "error", err)
		return ""
	}
	return shortnerResponse.ShortenedUrl
}
