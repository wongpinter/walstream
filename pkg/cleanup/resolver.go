package cleanup

import (
	"fmt"
	"sort"
)

// dependencyResolver implements DependencyResolver interface
type dependencyResolver struct {
	resources    map[string]ResourceInfo
	dependencies map[string]map[string]bool // resource -> dependencies
	dependents   map[string]map[string]bool // resource -> dependents
}

// NewDependencyResolver creates a new dependency resolver
func NewDependencyResolver() DependencyResolver {
	return &dependencyResolver{
		resources:    make(map[string]ResourceInfo),
		dependencies: make(map[string]map[string]bool),
		dependents:   make(map[string]map[string]bool),
	}
}

// AddResource implements DependencyResolver
func (dr *dependencyResolver) AddResource(res ResourceInfo) {
	resKey := fmt.Sprintf("%s/%s", res.Type, res.Name)
	dr.resources[resKey] = res

	// Initialize dependency maps for this resource
	if _, exists := dr.dependencies[resKey]; !exists {
		dr.dependencies[resKey] = make(map[string]bool)
	}
	if _, exists := dr.dependents[resKey]; !exists {
		dr.dependents[resKey] = make(map[string]bool)
	}

	// Add dependencies
	for _, dep := range res.DependsOn {
		depKey := fmt.Sprintf("%s/%s", res.Type, dep)
		dr.dependencies[resKey][depKey] = true
		if _, exists := dr.dependents[depKey]; !exists {
			dr.dependents[depKey] = make(map[string]bool)
		}
		dr.dependents[depKey][resKey] = true
	}

	// Add dependents
	for _, reqBy := range res.RequiredFor {
		reqKey := fmt.Sprintf("%s/%s", res.Type, reqBy)
		dr.dependents[resKey][reqKey] = true
		if _, exists := dr.dependencies[reqKey]; !exists {
			dr.dependencies[reqKey] = make(map[string]bool)
		}
		dr.dependencies[reqKey][resKey] = true
	}
}

// GetDeletionOrder implements DependencyResolver
func (dr *dependencyResolver) GetDeletionOrder() []ResourceInfo {
	// Use a topological sort to determine deletion order
	visited := make(map[string]bool)
	temp := make(map[string]bool) // For cycle detection
	var order []ResourceInfo

	var visit func(string)
	visit = func(resKey string) {
		if _, ok := temp[resKey]; ok {
			// Cycle detected, but don't fail here
			return
		}
		if visited[resKey] {
			return
		}

		temp[resKey] = true

		// Visit all dependents first
		for dep := range dr.dependents[resKey] {
			visit(dep)
		}

		delete(temp, resKey)
		visited[resKey] = true
		order = append(order, dr.resources[resKey])
	}

	// Create a predictable order for resource processing
	var keys []string
	for k := range dr.resources {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Visit all nodes
	for _, key := range keys {
		if !visited[key] {
			visit(key)
		}
	}

	// Reverse the order since we want dependents first
	for i, j := 0, len(order)-1; i < j; i, j = i+1, j-1 {
		order[i], order[j] = order[j], order[i]
	}

	// clean empty resources
	var cleanedOrder []ResourceInfo
	for _, res := range order {
		if res.Type != "" && res.Name != "" {
			cleanedOrder = append(cleanedOrder, res)
		}
	}

	order = cleanedOrder

	return order
}

// ValidateNoCycles implements DependencyResolver
func (dr *dependencyResolver) ValidateNoCycles() error {
	visited := make(map[string]bool)
	temp := make(map[string]bool)

	var checkCycle func(string) error
	checkCycle = func(resKey string) error {
		if temp[resKey] {
			return fmt.Errorf("circular dependency detected involving resource %s", resKey)
		}
		if visited[resKey] {
			return nil
		}

		temp[resKey] = true
		visited[resKey] = true

		for dep := range dr.dependencies[resKey] {
			if err := checkCycle(dep); err != nil {
				return fmt.Errorf("dependency chain: %s -> %s: %w", resKey, dep, err)
			}
		}

		delete(temp, resKey)
		return nil
	}

	// Check all resources
	for resKey := range dr.resources {
		if !visited[resKey] {
			if err := checkCycle(resKey); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetDependencyChain returns the chain of dependencies for a resource
func (dr *dependencyResolver) GetDependencyChain(resKey string) []string {
	var chain []string
	visited := make(map[string]bool)

	var traverse func(string)
	traverse = func(key string) {
		if visited[key] {
			return
		}
		visited[key] = true
		chain = append(chain, key)

		for dep := range dr.dependencies[key] {
			traverse(dep)
		}
	}

	traverse(resKey)
	return chain
}

// validateResourceExists verifies a resource exists in the resolver
func (dr *dependencyResolver) ValidateResourceExists(resKey string) bool {
	_, exists := dr.resources[resKey]
	return exists
}
