package core

import (
	"context"
	asynxmd "github.com/char2cs/asynx/models"
	"sync"
)

// Shutdown stops accepting work, waits for the commands and Forgets in flight,
// then closes the dispatcher and the bus. If ctx ends first it returns the
// error and can be called again to finish; once it has completed, or while
// another call runs, it returns ErrAlreadyShuttingDown.
func (p *Processor[T]) Shutdown(
	ctx context.Context,
) error {
	p.lifeMu.Lock()
	if p.shutdownStarted() {
		p.lifeMu.Unlock()
		return asynxmd.ErrAlreadyShuttingDown
	}

	p.markShuttingDown()
	p.lifeMu.Unlock()

	err := p.runShutdown(ctx)
	p.endShutdown(err)
	return err
}

func (p *Processor[T]) runShutdown(
	ctx context.Context,
) error {
	if p.dispatcher != nil {
		p.dispatcher.Interrupt()
	}

	if err := waitGroup(
		ctx,
		&p.sends,
	); err != nil {
		return err
	}

	if err := p.pool.Drain(ctx); err != nil {
		return err
	}

	if err := waitGroup(
		ctx,
		&p.forgets,
	); err != nil {
		return err
	}

	if err := p.closeDispatcher(ctx); err != nil {
		return err
	}

	return p.closeBus(ctx)
}

func (p *Processor[T]) shutdownStarted() bool {
	return p.shutdown == shuttingDownNow ||
		p.shutdown == shutDown
}

func (p *Processor[T]) markShuttingDown() {
	p.shutdown = shuttingDownNow
	p.shuttingDown.Store(true)
}

func (p *Processor[T]) endShutdown(
	err error,
) {
	p.lifeMu.Lock()
	defer p.lifeMu.Unlock()
	if err != nil {
		p.shutdown = shutdownFailed
		return
	}

	p.shutdown = shutDown
}

func waitGroup(
	ctx context.Context,
	wg *sync.WaitGroup,
) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Processor[T]) closeDispatcher(
	ctx context.Context,
) error {
	if p.dispatcher == nil {
		return nil
	}

	return p.dispatcher.Close(ctx)
}

func (p *Processor[T]) closeBus(
	ctx context.Context,
) error {
	if p.bus == nil {
		return nil
	}

	return p.bus.Close(ctx)
}

// ForTesting: WaitPublish blocks until all dispatched events have been delivered.
// Do not call in production code.
func (p *Processor[T]) WaitPublish() {
	if p.dispatcher != nil {
		p.dispatcher.WaitIdle()
	}
}
