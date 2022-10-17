package dataset

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"

	"github.com/Seann-Moser/QueryHelper/table/dataset_table"
)

type Cache interface {
	Get(args map[string]interface{}, table ...dataset_table.Table) (*sqlx.Rows, error)
	Set(rows *sqlx.Rows, args map[string]interface{}, table ...dataset_table.Table)
}

type GoCache struct {
	cache        map[string]*cacheSearch
	expiresCache time.Duration
	logger       *zap.Logger
}

type cacheSearch struct {
	internalCache map[string]*rowCache
}
type rowCache struct {
	rows       *sqlx.Rows
	cachedTime time.Time
}

var cacheNotFound = errors.New("unable to find cache for key")

var _ Cache = &GoCache{}

func NewGoCache(ctx context.Context, defaultExpiresTime time.Duration, logger *zap.Logger) *GoCache {
	c := &GoCache{cache: map[string]*cacheSearch{}, expiresCache: defaultExpiresTime, logger: logger}
	go func() { c.bustCache(ctx) }()
	return c
}

func (g GoCache) bustCache(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			for _, search := range g.cache {
				for k, v := range search.internalCache {
					if v.cachedTime.Before(time.Now().Add(-1 * g.expiresCache)) {
						search.internalCache[k] = nil
						g.logger.Debug("busting cache", zap.String("args", k))
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (g GoCache) Get(args map[string]interface{}, table ...dataset_table.Table) (*sqlx.Rows, error) {
	v, found := g.cache[getTableKey(table...)]
	if !found {
		return nil, cacheNotFound
	}
	rows, found := v.internalCache[getArgKey(args)]
	if !found {
		return nil, cacheNotFound
	}
	if rows == nil {
		return nil, cacheNotFound
	}
	g.logger.Debug("getting cache")
	return rows.rows, nil
}

func (g GoCache) Set(rows *sqlx.Rows, args map[string]interface{}, table ...dataset_table.Table) {
	v, found := g.cache[getTableKey(table...)]
	if !found {
		v = &cacheSearch{internalCache: map[string]*rowCache{}}
		g.cache[getTableKey(table...)] = v
	}
	v.internalCache[getArgKey(args)] = &rowCache{
		rows:       rows,
		cachedTime: time.Now(),
	}
	g.logger.Debug("setting cache")
}

func getArgKey(args map[string]interface{}) string {
	key := ""
	for k, v := range args {
		key += fmt.Sprintf("%s:%v", k, v)
	}
	sha := sha1.New()
	return base64.StdEncoding.EncodeToString(sha.Sum([]byte(key)))
}

func getTableKey(table ...dataset_table.Table) string {
	key := ""
	for _, v := range table {
		key += v.FullTableName()
	}
	sha := sha1.New()
	return base64.StdEncoding.EncodeToString(sha.Sum([]byte(key)))
}
