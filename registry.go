package orca

import (
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// algorithmRegistry keeps a local copy of all registered algorithms from
// this processor
//
// thread safe through the use of a RWMutex
type algorithmRegistry struct {
	// a rw mutex might be overkill here. the registry is not containing
	// ephemeral data. it registers once and gets on its way. this lock
	// only protects against go routine algorithm registration
	mu                 sync.RWMutex
	algorithms         map[string]*Algorithm
	dependencies       map[string][]*Algorithm
	remoteDependencies map[string][]*RemoteAlgorithm
	windowTriggers     map[string][]*Algorithm
	lookbacks          map[string]map[string]lookbackParams
}

type lookbackParams struct {
	N     int
	TD    time.Duration
	GapN  int
	GapTD time.Duration
}

func newAlgorithmRegistry() *algorithmRegistry {
	return &algorithmRegistry{
		algorithms:         make(map[string]*Algorithm),
		dependencies:       make(map[string][]*Algorithm),
		remoteDependencies: make(map[string][]*RemoteAlgorithm),
		windowTriggers:     make(map[string][]*Algorithm),
		lookbacks:          make(map[string]map[string]lookbackParams),
	}
}

func (r *algorithmRegistry) addAlgorithm(algo *Algorithm) error {
	r.mu.Lock()

	defer r.mu.Unlock()

	if _, exists := r.algorithms[algo.FullName()]; exists {
		return InvalidAlgorithmArgumentError{fmt.Sprintf("algorithm %s already exists", algo.FullName())}
	}
	log.Info().Str("algorithmName", algo.Name).Str("windowName", algo.WindowType.Name).Msg("registering algorithm")

	r.algorithms[algo.FullName()] = algo

	return nil
}

func (r *algorithmRegistry) addDependency(algorithm *Algorithm, dependant *Algorithm, lookback lookbackParams) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// find the dependency algorithm
	if _, found := r.algorithms[dependant.FullName()]; !found {
		return InvalidDependencyError{"dependency not found"}
	}

	r.dependencies[algorithm.FullName()] = append(r.dependencies[algorithm.FullName()], dependant)
	r.addLookback(algorithm.FullName(), dependant.FullName(), lookback)
	return nil
}

func (r *algorithmRegistry) addRemoteDependency(algorithm *Algorithm, dependant *RemoteAlgorithm, lookback lookbackParams) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.remoteDependencies[algorithm.FullName()] = append(r.remoteDependencies[algorithm.FullName()], dependant)
	r.addLookback(algorithm.FullName(), dependant.FullName(), lookback)

	return nil
}

func (r *algorithmRegistry) addWindowTrigger(windowName string, algo *Algorithm) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.windowTriggers[windowName] = append(r.windowTriggers[windowName], algo)
}

func (r *algorithmRegistry) hasAlgorithmFunc(fn AlgorithmFunc) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, algo := range r.algorithms {
		if isSameFunc(algo.ExecFunc, fn) {
			return true
		}
	}
	return false
}

func (r *algorithmRegistry) addLookback(from, to string, params lookbackParams) {
	if params.N == 0 && params.TD == 0 {
		return
	}
	if r.lookbacks[from] == nil {
		r.lookbacks[from] = make(map[string]lookbackParams)
	}
	r.lookbacks[from][to] = params
}

func (r *algorithmRegistry) getLookback(from, to string) lookbackParams {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if toMap, exists := r.lookbacks[from]; exists {
		if params, exists := toMap[to]; exists {
			return params
		}
	}
	return lookbackParams{N: 0, TD: 0}
}
