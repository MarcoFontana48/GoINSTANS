package main

import (
	"fmt"
	"strings"
	"sync"
)

// Core types and data structures

// Fact represents immutable fact with attributes
type Fact struct {
	ID         string
	Attributes map[string]interface{}
}

// NewFact creates a new fact with given ID and attributes
func NewFact(id string, attributes map[string]interface{}) *Fact {
	if attributes == nil {
		attributes = make(map[string]interface{})
	}
	return &Fact{
		ID:         id,
		Attributes: attributes,
	}
}

// GetAttribute retrieves an attribute value
func (f *Fact) GetAttribute(key string) (interface{}, bool) {
	val, exists := f.Attributes[key]
	return val, exists
}

// WithAttribute creates a new fact with additional attribute (immutable pattern)
func (f *Fact) WithAttribute(key string, value interface{}) *Fact {
	newAttrs := make(map[string]interface{})
	for k, v := range f.Attributes {
		newAttrs[k] = v
	}
	newAttrs[key] = value
	return &Fact{
		ID:         f.ID,
		Attributes: newAttrs,
	}
}

// Token represents a collection of facts in the network
type Token struct {
	Facts  []*Fact
	Parent *Token
}

// NewToken creates a new token
func NewToken(facts []*Fact, parent *Token) *Token {
	return &Token{
		Facts:  facts,
		Parent: parent,
	}
}

// EmptyToken creates an empty token
func EmptyToken() *Token {
	return &Token{
		Facts:  make([]*Fact, 0),
		Parent: nil,
	}
}

// AddFact creates a new token with an additional fact
func (t *Token) AddFact(fact *Fact) *Token {
	newFacts := make([]*Fact, len(t.Facts)+1)
	copy(newFacts, t.Facts)
	newFacts[len(t.Facts)] = fact
	return NewToken(newFacts, t)
}

// IsEmpty checks if token has no facts
func (t *Token) IsEmpty() bool {
	return len(t.Facts) == 0
}

// Type aliases for clarity
type NodeID string
type JoinCondition func(*Token, *Fact) bool
type RuleAction func(*Token)

// ReteNode interface defines the contract for all nodes
type ReteNode interface {
	GetID() NodeID
	AddLeftChild(ReteNode)
	AddRightChild(ReteNode)
	LeftActivate(*Token)
	RightActivate(*Fact)
}

// BaseNode provides common functionality for all nodes
type BaseNode struct {
	ID            NodeID
	LeftChildren  []ReteNode
	RightChildren []ReteNode
	mutex         sync.RWMutex
}

// GetID returns the node ID
func (b *BaseNode) GetID() NodeID {
	return b.ID
}

// AddLeftChild adds a left child node
func (b *BaseNode) AddLeftChild(child ReteNode) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.LeftChildren = append(b.LeftChildren, child)
}

// AddRightChild adds a right child node
func (b *BaseNode) AddRightChild(child ReteNode) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.RightChildren = append(b.RightChildren, child)
}

// PropagateLeft propagates token to left children
func (b *BaseNode) PropagateLeft(token *Token) {
	b.mutex.RLock()
	children := make([]ReteNode, len(b.LeftChildren))
	copy(children, b.LeftChildren)
	b.mutex.RUnlock()

	for _, child := range children {
		child.LeftActivate(token)
	}
}

// PropagateRight propagates fact to right children
func (b *BaseNode) PropagateRight(fact *Fact) {
	b.mutex.RLock()
	children := make([]ReteNode, len(b.RightChildren))
	copy(children, b.RightChildren)
	b.mutex.RUnlock()

	for _, child := range children {
		child.RightActivate(fact)
	}
}

// RootNode acts as the entry point for the network
type RootNode struct {
	*BaseNode
	leftMemory map[*Token]bool
	mutex      sync.RWMutex
}

// NewRootNode creates a new root node
func NewRootNode() *RootNode {
	return &RootNode{
		BaseNode: &BaseNode{
			ID:            "root",
			LeftChildren:  make([]ReteNode, 0),
			RightChildren: make([]ReteNode, 0),
		},
		leftMemory: make(map[*Token]bool),
	}
}

// LeftActivate processes left activation
func (r *RootNode) LeftActivate(token *Token) {
	r.mutex.Lock()
	r.leftMemory[token] = true
	r.mutex.Unlock()

	r.PropagateLeft(token)
}

// RightActivate processes right activation
func (r *RootNode) RightActivate(fact *Fact) {
	// Send facts only to alpha nodes (right children)
	r.mutex.RLock()
	children := make([]ReteNode, len(r.RightChildren))
	copy(children, r.RightChildren)
	r.mutex.RUnlock()

	for _, child := range children {
		if alphaNode, ok := child.(*AlphaNode); ok {
			alphaNode.RightActivate(fact)
		}
	}
}

// AddFact adds a fact to the network
func (r *RootNode) AddFact(fact *Fact) {
	r.RightActivate(fact)
}

// Initialize initializes the network with an empty token
func (r *RootNode) Initialize() {
	r.LeftActivate(EmptyToken())
}

// GetLeftMemory returns a copy of the left memory
func (r *RootNode) GetLeftMemory() []*Token {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	tokens := make([]*Token, 0, len(r.leftMemory))
	for token := range r.leftMemory {
		tokens = append(tokens, token)
	}
	return tokens
}

// AlphaNode filters facts based on conditions
type AlphaNode struct {
	*BaseNode
	condition func(*Fact) bool
	memory    map[*Fact]bool
	mutex     sync.RWMutex
}

// NewAlphaNode creates a new alpha node
func NewAlphaNode(id NodeID, condition func(*Fact) bool) *AlphaNode {
	return &AlphaNode{
		BaseNode: &BaseNode{
			ID:            id,
			LeftChildren:  make([]ReteNode, 0),
			RightChildren: make([]ReteNode, 0),
		},
		condition: condition,
		memory:    make(map[*Fact]bool),
	}
}

// NewAlphaNodeWithAttribute creates an alpha node that tests for a specific attribute value
func NewAlphaNodeWithAttribute(id NodeID, attribute string, value interface{}) *AlphaNode {
	condition := func(fact *Fact) bool {
		val, exists := fact.GetAttribute(attribute)
		return exists && val == value
	}
	return NewAlphaNode(id, condition)
}

// LeftActivate processes left activation (alpha nodes don't process these)
func (a *AlphaNode) LeftActivate(token *Token) {
	// Alpha nodes don't process left activations
}

// RightActivate processes right activation
func (a *AlphaNode) RightActivate(fact *Fact) {
	fmt.Printf("AlphaNode %s: Testing fact %s\n", a.ID, fact.ID)

	if a.condition(fact) {
		fmt.Printf("AlphaNode %s: Fact %s passed test\n", a.ID, fact.ID)

		a.mutex.Lock()
		a.memory[fact] = true
		a.mutex.Unlock()

		a.PropagateRight(fact)
	} else {
		fmt.Printf("AlphaNode %s: Fact %s failed test\n", a.ID, fact.ID)
	}
}

// GetMemory returns a copy of the memory
func (a *AlphaNode) GetMemory() []*Fact {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	facts := make([]*Fact, 0, len(a.memory))
	for fact := range a.memory {
		facts = append(facts, fact)
	}
	return facts
}

// BetaNode performs joins between tokens and facts
type BetaNode struct {
	*BaseNode
	joinCondition JoinCondition
	leftMemory    map[*Token]bool
	rightMemory   map[*Fact]bool
	mutex         sync.RWMutex
}

// NewBetaNode creates a new beta node
func NewBetaNode(id NodeID, joinCondition JoinCondition) *BetaNode {
	return &BetaNode{
		BaseNode: &BaseNode{
			ID:            id,
			LeftChildren:  make([]ReteNode, 0),
			RightChildren: make([]ReteNode, 0),
		},
		joinCondition: joinCondition,
		leftMemory:    make(map[*Token]bool),
		rightMemory:   make(map[*Fact]bool),
	}
}

// LeftActivate processes left activation
func (b *BetaNode) LeftActivate(token *Token) {
	fmt.Printf("BetaNode %s: Left activation with token containing %d facts\n", b.ID, len(token.Facts))

	b.mutex.Lock()
	b.leftMemory[token] = true
	rightFacts := make([]*Fact, 0, len(b.rightMemory))
	for fact := range b.rightMemory {
		rightFacts = append(rightFacts, fact)
	}
	b.mutex.Unlock()

	// Try to join with all facts in right memory
	for _, fact := range rightFacts {
		if b.joinCondition == nil || b.joinCondition(token, fact) {
			if b.joinCondition != nil {
				fmt.Printf("BetaNode %s: Join condition result for token-fact pair: true\n", b.ID)
			}
			newToken := token.AddFact(fact)
			fmt.Printf("BetaNode %s: Successful join, propagating token with %d facts\n", b.ID, len(newToken.Facts))
			b.PropagateLeft(newToken)
		}
	}
}

// RightActivate processes right activation
func (b *BetaNode) RightActivate(fact *Fact) {
	fmt.Printf("BetaNode %s: Right activation with fact %s\n", b.ID, fact.ID)

	b.mutex.Lock()
	b.rightMemory[fact] = true
	leftTokens := make([]*Token, 0, len(b.leftMemory))
	for token := range b.leftMemory {
		leftTokens = append(leftTokens, token)
	}
	b.mutex.Unlock()

	// Try to join with all tokens in left memory
	for _, token := range leftTokens {
		if b.joinCondition == nil || b.joinCondition(token, fact) {
			if b.joinCondition != nil {
				fmt.Printf("BetaNode %s: Join condition result for token-fact pair: true\n", b.ID)
			}
			newToken := token.AddFact(fact)
			fmt.Printf("BetaNode %s: Successful join, propagating token with %d facts\n", b.ID, len(newToken.Facts))
			b.PropagateLeft(newToken)
		}
	}
}

// GetLeftMemory returns a copy of the left memory
func (b *BetaNode) GetLeftMemory() []*Token {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	tokens := make([]*Token, 0, len(b.leftMemory))
	for token := range b.leftMemory {
		tokens = append(tokens, token)
	}
	return tokens
}

// GetRightMemory returns a copy of the right memory
func (b *BetaNode) GetRightMemory() []*Fact {
	b.mutex.RLock()
	defer b.mutex.RUnlock()

	facts := make([]*Fact, 0, len(b.rightMemory))
	for fact := range b.rightMemory {
		facts = append(facts, fact)
	}
	return facts
}

// SameIDJoinCondition creates a join condition that matches facts with the same ID
func SameIDJoinCondition(token *Token, fact *Fact) bool {
	for _, tokenFact := range token.Facts {
		tokenID, tokenHasID := tokenFact.GetAttribute("id")
		factID, factHasID := fact.GetAttribute("id")

		fmt.Printf("Comparing token fact %s (id=%v) with fact %s (id=%v)\n",
			tokenFact.ID, tokenID, fact.ID, factID)

		if tokenHasID && factHasID && tokenID == factID {
			return true
		}
	}
	return false
}

// TerminalNode executes rule actions
type TerminalNode struct {
	*BaseNode
	ruleName string
	action   RuleAction
	memory   map[*Token]bool
	mutex    sync.RWMutex
}

// NewTerminalNode creates a new terminal node
func NewTerminalNode(id NodeID, ruleName string, action RuleAction) *TerminalNode {
	return &TerminalNode{
		BaseNode: &BaseNode{
			ID:            id,
			LeftChildren:  make([]ReteNode, 0),
			RightChildren: make([]ReteNode, 0),
		},
		ruleName: ruleName,
		action:   action,
		memory:   make(map[*Token]bool),
	}
}

// LeftActivate processes left activation
func (t *TerminalNode) LeftActivate(token *Token) {
	fmt.Printf("TerminalNode %s: Activated with token containing %d facts\n", t.ID, len(token.Facts))

	t.mutex.Lock()
	t.memory[token] = true
	t.mutex.Unlock()

	if t.action != nil {
		t.action(token)
	}

	factIDs := make([]string, len(token.Facts))
	for i, fact := range token.Facts {
		factIDs[i] = fact.ID
	}
	fmt.Printf("Rule '%s' fired with facts: %s\n", t.ruleName, strings.Join(factIDs, ", "))
}

// RightActivate processes right activation (terminal nodes don't process these)
func (t *TerminalNode) RightActivate(fact *Fact) {
	// Terminal nodes don't process right activations
}

// GetMemory returns a copy of the memory
func (t *TerminalNode) GetMemory() []*Token {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	tokens := make([]*Token, 0, len(t.memory))
	for token := range t.memory {
		tokens = append(tokens, token)
	}
	return tokens
}

// ReteEngine manages the entire Rete network
type ReteEngine struct {
	root  *RootNode
	nodes map[NodeID]ReteNode
	mutex sync.RWMutex
}

// NewReteEngine creates a new Rete engine
func NewReteEngine() *ReteEngine {
	root := NewRootNode()
	return &ReteEngine{
		root:  root,
		nodes: map[NodeID]ReteNode{"root": root},
	}
}

// AddAlphaNode adds an alpha node to the network
func (e *ReteEngine) AddAlphaNode(id NodeID, attribute string, value interface{}) *ReteEngine {
	alphaNode := NewAlphaNodeWithAttribute(id, attribute, value)
	e.root.AddRightChild(alphaNode)

	e.mutex.Lock()
	e.nodes[id] = alphaNode
	e.mutex.Unlock()

	return e
}

// AddBetaNode adds a beta node to the network
func (e *ReteEngine) AddBetaNode(id NodeID, leftParentID NodeID, rightParentID NodeID, joinCondition JoinCondition) *ReteEngine {
	betaNode := NewBetaNode(id, joinCondition)

	e.mutex.RLock()
	leftParent, leftExists := e.nodes[leftParentID]
	rightParent, rightExists := e.nodes[rightParentID]
	e.mutex.RUnlock()

	if !leftExists {
		panic(fmt.Sprintf("Left parent node %s not found", leftParentID))
	}
	if !rightExists {
		panic(fmt.Sprintf("Right parent node %s not found", rightParentID))
	}

	// Connect to left parent (receives tokens)
	leftParent.AddLeftChild(betaNode)

	// Connect to right parent (receives facts) - only if different from left parent
	if leftParentID != rightParentID {
		rightParent.AddRightChild(betaNode)
	}

	e.mutex.Lock()
	e.nodes[id] = betaNode
	e.mutex.Unlock()

	return e
}

// AddTerminalNode adds a terminal node to the network
func (e *ReteEngine) AddTerminalNode(id NodeID, parentID NodeID, ruleName string, action RuleAction) *ReteEngine {
	terminalNode := NewTerminalNode(id, ruleName, action)

	e.mutex.RLock()
	parent, exists := e.nodes[parentID]
	e.mutex.RUnlock()

	if !exists {
		panic(fmt.Sprintf("Parent node %s not found", parentID))
	}

	parent.AddLeftChild(terminalNode)

	e.mutex.Lock()
	e.nodes[id] = terminalNode
	e.mutex.Unlock()

	return e
}

// AddFact adds a fact to the network
func (e *ReteEngine) AddFact(fact *Fact) *ReteEngine {
	fmt.Printf("\n=== Adding fact: %s ===\n", fact.ID)
	e.root.AddFact(fact)
	return e
}

// Initialize initializes the network with an empty token
func (e *ReteEngine) Initialize() *ReteEngine {
	fmt.Println("=== Initializing network with empty token ===")
	e.root.Initialize()
	return e
}

// GetNode returns a node by ID
func (e *ReteEngine) GetNode(id NodeID) ReteNode {
	e.mutex.RLock()
	defer e.mutex.RUnlock()
	return e.nodes[id]
}

// GetAlphaMemory returns the memory of an alpha node
func (e *ReteEngine) GetAlphaMemory(id NodeID) []*Fact {
	if node := e.GetNode(id); node != nil {
		if alphaNode, ok := node.(*AlphaNode); ok {
			return alphaNode.GetMemory()
		}
	}
	return nil
}

// GetBetaLeftMemory returns the left memory of a beta node
func (e *ReteEngine) GetBetaLeftMemory(id NodeID) []*Token {
	if node := e.GetNode(id); node != nil {
		if betaNode, ok := node.(*BetaNode); ok {
			return betaNode.GetLeftMemory()
		}
	}
	return nil
}

// GetBetaRightMemory returns the right memory of a beta node
func (e *ReteEngine) GetBetaRightMemory(id NodeID) []*Fact {
	if node := e.GetNode(id); node != nil {
		if betaNode, ok := node.(*BetaNode); ok {
			return betaNode.GetRightMemory()
		}
	}
	return nil
}

// GetTerminalMemory returns the memory of a terminal node
func (e *ReteEngine) GetTerminalMemory(id NodeID) []*Token {
	if node := e.GetNode(id); node != nil {
		if terminalNode, ok := node.(*TerminalNode); ok {
			return terminalNode.GetMemory()
		}
	}
	return nil
}

// GetRootLeftMemory returns the left memory of the root node
func (e *ReteEngine) GetRootLeftMemory() []*Token {
	return e.root.GetLeftMemory()
}

// Helper function to create facts
func CreateFact(id string, attrs map[string]interface{}) *Fact {
	return NewFact(id, attrs)
}

// Example usage
func main() {
	// Create the engine and build network
	fmt.Println("=== Building Rete Network ===")

	engine := NewReteEngine().
		AddAlphaNode("alpha1", "type", "person").
		AddAlphaNode("alpha2", "age_group", "adult").
		AddAlphaNode("alpha3", "likes", "coffee").
		AddBetaNode("beta1", "root", "alpha1", nil).                  // Join root (empty token) with person facts
		AddBetaNode("beta2", "beta1", "alpha2", SameIDJoinCondition). // Join person tokens with adult facts
		AddBetaNode("beta3", "beta2", "alpha3", SameIDJoinCondition). // Join adult person tokens with coffee facts
		AddTerminalNode(
			"terminal1",
			"beta3",
			"CoffeeDrinkerRule",
			func(token *Token) {
				var personName interface{} = "unknown"
				for _, fact := range token.Facts {
					if name, exists := fact.GetAttribute("name"); exists {
						personName = name
						break
					}
				}
				fmt.Printf("Action: %v is a coffee drinker!\n", personName)
			},
		).
		Initialize()

	// Test data
	facts := []*Fact{
		CreateFact("john_person", map[string]interface{}{"id": "john", "type": "person", "name": "John"}),
		CreateFact("john_age", map[string]interface{}{"id": "john", "age_group": "adult", "age": 30}),
		CreateFact("john_likes", map[string]interface{}{"id": "john", "likes": "coffee"}),
		CreateFact("mary_person", map[string]interface{}{"id": "mary", "type": "person", "name": "Mary"}),
		CreateFact("mary_age", map[string]interface{}{"id": "mary", "age_group": "adult", "age": 25}),
		CreateFact("mary_likes", map[string]interface{}{"id": "mary", "likes": "tea"}),
	}

	fmt.Println("\n=== Adding facts to the system ===")

	// Process facts
	for _, fact := range facts {
		engine.AddFact(fact)
	}

	fmt.Println("\n=== Final Memory State ===")

	// Display memory contents
	fmt.Printf("Root left memory: %d tokens\n", len(engine.GetRootLeftMemory()))

	if alpha1Memory := engine.GetAlphaMemory("alpha1"); alpha1Memory != nil {
		fmt.Printf("Alpha1 memory (type=person): %d facts\n", len(alpha1Memory))
	}
	if alpha2Memory := engine.GetAlphaMemory("alpha2"); alpha2Memory != nil {
		fmt.Printf("Alpha2 memory (age_group=adult): %d facts\n", len(alpha2Memory))
	}
	if alpha3Memory := engine.GetAlphaMemory("alpha3"); alpha3Memory != nil {
		fmt.Printf("Alpha3 memory (likes=coffee): %d facts\n", len(alpha3Memory))
	}

	// Display beta node memories
	fmt.Println("\nBeta node memories:")
	if beta1Left := engine.GetBetaLeftMemory("beta1"); beta1Left != nil {
		fmt.Printf("Beta1 left memory: %d tokens\n", len(beta1Left))
	}
	if beta1Right := engine.GetBetaRightMemory("beta1"); beta1Right != nil {
		fmt.Printf("Beta1 right memory: %d facts\n", len(beta1Right))
	}
	if beta2Left := engine.GetBetaLeftMemory("beta2"); beta2Left != nil {
		fmt.Printf("Beta2 left memory: %d tokens\n", len(beta2Left))
	}
	if beta2Right := engine.GetBetaRightMemory("beta2"); beta2Right != nil {
		fmt.Printf("Beta2 right memory: %d facts\n", len(beta2Right))
	}
	if beta3Left := engine.GetBetaLeftMemory("beta3"); beta3Left != nil {
		fmt.Printf("Beta3 left memory: %d tokens\n", len(beta3Left))
	}
	if beta3Right := engine.GetBetaRightMemory("beta3"); beta3Right != nil {
		fmt.Printf("Beta3 right memory: %d facts\n", len(beta3Right))
	}

	// Display terminal memory
	if terminalMemory := engine.GetTerminalMemory("terminal1"); terminalMemory != nil {
		fmt.Printf("Terminal memory: %d tokens\n", len(terminalMemory))
	}

	// Demonstrate query capabilities
	coffeeMemory := engine.GetAlphaMemory("alpha3")
	coffeeLovers := make([]string, 0)
	if coffeeMemory != nil {
		for _, fact := range coffeeMemory {
			if id, exists := fact.GetAttribute("id"); exists {
				if idStr, ok := id.(string); ok {
					coffeeLovers = append(coffeeLovers, idStr)
				}
			}
		}
	}

	fmt.Printf("\nCoffee lovers identified: %s\n", strings.Join(coffeeLovers, ", "))
}
