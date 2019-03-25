// Copyright 2010 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
//
// Modifications, Annotations and explanations C.E. Thornton
// The document expaining this program and components is available at:
//    "hawthornepresscom@gmail.com" under Documents.
//
package main

import (
	"container/heap"
	"fmt"
	"math/rand"
	"time"
)

// Number of Requester GO Routines and number of Worker GO Routines
const nRequester = 100
const nWorker = 10

// Request Structure
type Request struct {
	fn func() int // Work Function to call
	c  chan int   // Reply Channel(Tells Request Function 'work done')
}

// "Request" Goroutine
//    Infinite Loop - Wait ... Send Request ... Wait for done
//  "Requester" Creates the Work Function, Done, and places in Work Channel
func requester(work chan Request) {
	c := make(chan int) // Make Reply Channel
	for {               // Loop Forever
		time.Sleep(time.Duration(rand.Int63n(nWorker * 2e9))) // Random Wait
		work <- Request{op, c}                                // Push Request into "Work" Channel
		<-c                                                   // Wait for "Done" Reply
	}
}

// All the Work Requests instantiate the "op()" function below.
// Simulation of some work: just sleep for a while and report how long.
//
func op() int { // Actual Simulated Work Function
	n := rand.Int63n(1e9)
	time.Sleep(time.Duration(nWorker * n)) // Sleep random amount
	return int(n)                          // Return time slept(value not used)
}

// Worker Structure: Holds Requests, Index into Pool Queue, and Job Count
type Worker struct {
	i        int          // Index into the Pool Structure
	requests chan Request // Worker Request Value
	pending  int          // Pending Job Count Value
}

// The "work" Method places executes the worker function and waits till
// completed and sends the *Worker value to the done channel
func (w *Worker) work(done chan *Worker) {
	for {
		req := <-w.requests // Get Request Channel
		req.c <- req.fn()   // Send Function Call to Channel
		done <- w           // Wait for Function to complete
	}
}

// Pool Slice (Implements Priority Queue via HEAP Interface!)
//
type Pool []*Worker // Create Slice of Pointers to Worker Structures

// The following routines implement the HEAP Interface.
// For an explanation for the Package Heap: "container/heap"
//
func (p Pool) Len() int { return len(p) } // Return length of Pool

func (p Pool) Less(i, j int) bool {
	return p[i].pending < p[j].pending // Return Compare of pending values
}

func (p *Pool) Swap(i, j int) {
	a := *p
	a[i], a[j] = a[j], a[i] // Swap Worker Structures
	a[i].i = i              // Adjust Both Pool Indexes
	a[j].i = j
}

func (p *Pool) Push(x interface{}) {
	a := *p          // Get base of Pool Structure
	n := len(a)      // Len of input Slice
	a = a[0 : n+1]   // Create New Element at end+1 of slice
	w := x.(*Worker) // w now equal *Worker Parameter
	a[n] = w         // Put Worker in new slot
	w.i = n          // Worker.i = position in Pool!
	*p = a           // Update Pool Slice!
}

func (p *Pool) Pop() interface{} {
	a := *p              // Get base of Pool Structure
	*p = a[0 : len(a)-1] // Shorten the Pool by 1
	w := a[len(a)-1]     // Load Removed Element
	w.i = -1             // for safety (Non-existant Pool Index)
	return w             // Return Last Pool Value
}

type Balancer struct {
	pool Pool
	done chan *Worker
}

// Create Pool and Start Work Goroutines
func NewBalancer() *Balancer {
	done := make(chan *Worker, nWorker)
	b := &Balancer{make(Pool, 0, nWorker), done}
	for i := 0; i < nWorker; i++ { // For Each Worker
		// Create a Worker Structure and Point to it
		w := &Worker{requests: make(chan Request, nRequester)}
		heap.Push(&b.pool, w) // PUSH Struture into Queue
		go w.work(b.done)     // Start work processing routine
	}
	return b // Return pointer to Balancer Structure
}

// Print Statistics:
//     This Function prints balancing statistics each time a worker
//     completes a task. It prints out the number of the pending requests per
//     worker and the average pending requests and their variance.
//
func (b *Balancer) print() {
	sum := 0
	sumsq := 0
	for _, w := range b.pool { //Loop thru the Pool
		fmt.Printf("%d ", w.pending) // Print Pending Count
		sum += w.pending             // Compute Intermediates
		sumsq += w.pending * w.pending
	}
	// Compute and Print Average and Variance of Pending Counts
	avg := float64(sum) / float64(len(b.pool))
	variance := float64(sumsq)/float64(len(b.pool)) - avg*avg
	fmt.Printf(" %.2f %.2f\n", avg, variance)
}

// Balance Loop Processing
func (b *Balancer) balance(work chan Request) {
	for { // Infinite Loop
		select { // Select on Channel
		case req := <-work: // Dispatch Requests
			b.dispatch(req)
		case w := <-b.done: // Process Completions
			b.completed(w)
		}
		b.print() // Print Statistics
	}
}

// These functions (dispatch and Completed):
// Dispatch finds Worker with the Lightest Load and sends it the request.
func (b *Balancer) dispatch(req Request) {
	w := heap.Pop(&b.pool).(*Worker) // Get Item with least/equal low value
	w.requests <- req                // Update Request Buffer
	w.pending++                      // Advance Pending Count (+1)
	heap.Push(&b.pool, w)            // Update Value in Heap!
}

// Completed: When a request is completed it is removed from the Pool,
// Then pushes the worker back into the Pool with a updated Pending Value.
func (b *Balancer) completed(w *Worker) {
	w.pending--               // Update Pending Value (-1)
	heap.Remove(&b.pool, w.i) // Remove Item at work index in pool
	heap.Push(&b.pool, w)     // Push back Item with a new pending value
}

// The main function:
// - creates Work Channel
// - Create and start Request Goroutines
// - Create Worker Pool and Start Worker Go Routines
// - launch balancer Loop
func main() {
	work := make(chan Request) // Create the "Work" Channel
	for i := 0; i < nRequester; i++ {
		go requester(work) // Create and start request Goroutines
	}
	b := NewBalancer() // Create Worker Pool & Start Workers Goroutines

	b.balance(work) // Launches Balancer Loop
}
