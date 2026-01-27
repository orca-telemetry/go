package orca_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

var testCtx context.Context
var orcaDbConnStr string
var orcaConnStr string
var cleanupFuncs []func()

func TestMain(m *testing.M) {
	testCtx = context.Background()

	var pgCleanup func()
	orcaDbConnStr, pgCleanup = setupPg(testCtx)
	cleanupFuncs = append(cleanupFuncs, pgCleanup)

	var orcaCleanup func()
	orcaConnStr, orcaCleanup = setupOrca(testCtx)
	cleanupFuncs = append(cleanupFuncs, orcaCleanup)

	// runs all tests
	code := m.Run()

	for i := len(cleanupFuncs) - 1; i >= 0; i-- {
		cleanupFuncs[i]()
	}

	os.Exit(code)
}

func setupPg(ctx context.Context) (string, func()) {
	postgresContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("test"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
	)
	if err != nil {
		panic("Failed to start postgres container: " + err.Error())
	}

	connStr, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic("Failed to get connection string: " + err.Error())
	}

	cleanup := func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			println("Failed to terminate postgres container:", err.Error())
		}
	}

	return connStr, cleanup
}

func setupOrca(ctx context.Context) (string, func()) {
	nw, err := network.New(ctx)
	if err != nil {
		panic("failed to create network: " + err.Error())
	}

	orcaContainer, err := testcontainers.Run(
		ctx,
		"ghcr.io/orca-telemetry/core:0.12.0",
		network.WithNetwork([]string{"orca-alias"}, nw),
		testcontainers.WithEnv(map[string]string{
			"ORCA_CONNECTION_STRING": orcaDbConnStr,
			"ORCA_PORT":              "4040",
			"ORCA_LOG_LEVEL":         "DEBUG",
		}),
		testcontainers.WithCmd("/app/orca", "-migrate"),
		testcontainers.WithExposedPorts("4040/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("4040/tcp").WithStartupTimeout(60*time.Second),
		),
	)

	if err != nil {
		panic("orca could not be started: " + err.Error())
	}

	endpoint, err := orcaContainer.Endpoint(ctx, "")
	if err != nil {
		panic("failed to get endpoint: " + err.Error())
	}

	cleanup := func() {
		if err := orcaContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate orca container: %s", err)
		}
		if err := nw.Remove(ctx); err != nil {
			log.Printf("failed to remove network: %s", err)
		}
	}

	return endpoint, cleanup
}
