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

	nw, err := network.New(testCtx)
	if err != nil {
		panic("failed to create network: " + err.Error())
	}

	var pgCleanup func()
	orcaDbConnStr, pgCleanup = setupPg(testCtx, nw)
	orcaDbConnStr = "postgres://user:password@db:5432/test?sslmode=disable"
	cleanupFuncs = append(cleanupFuncs, pgCleanup)

	var orcaCleanup func()
	orcaConnStr, orcaCleanup = setupOrca(testCtx, nw)
	cleanupFuncs = append(cleanupFuncs, orcaCleanup)

	// runs all tests
	code := m.Run()

	for i := len(cleanupFuncs) - 1; i >= 0; i-- {
		cleanupFuncs[i]()
	}

	os.Exit(code)
}

func setupPg(ctx context.Context, nw *testcontainers.DockerNetwork) (string, func()) {
	postgresContainer, err := postgres.Run(ctx,
		"postgres:17-alpine",
		postgres.WithDatabase("test"),
		postgres.WithUsername("user"),
		postgres.WithPassword("password"),
		postgres.BasicWaitStrategies(),
		postgres.WithSQLDriver("pgx"),
		network.WithNetwork([]string{"orca-nw"}, nw),
		testcontainers.WithName("db"),
	)
	if err != nil {
		panic("Failed to start postgres container: " + err.Error())
	}

	_, err = postgresContainer.ConnectionString(ctx, "sslmode=disable")

	// ignore the default conn. string
	if err != nil {
		panic("Failed to get connection string: " + err.Error())
	}

	cleanup := func() {
		if err := postgresContainer.Terminate(ctx); err != nil {
			println("Failed to terminate postgres container:", err.Error())
		}
	}

	return "postgres://user:password@db:5432/test?sslmode=disable", cleanup
}

func setupOrca(ctx context.Context, nw *testcontainers.DockerNetwork) (string, func()) {
	log.Info().Str("db conn str", orcaDbConnStr).Msg("connection string")
	orcaContainer, err := testcontainers.Run(
		ctx,
		"ghcr.io/orca-telemetry/core:latest",
		network.WithNetwork([]string{"orca-nw"}, nw),
		testcontainers.WithEnv(map[string]string{
			"ORCA_CONNECTION_STRING": orcaDbConnStr,
			"ORCA_PORT":              "4040",
			"ORCA_LOG_LEVEL":         "DEBUG",
		}),
		testcontainers.WithCmd("-migrate"),
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
	log.Info().Str("orca conn str", endpoint).Msg("connection string")

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
