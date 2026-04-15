package mocks

import "context"

// Store is a no-op implementation of models.Store. All operations succeed
// without storing or returning any data. Useful when a Store is required but
// its behavior is irrelevant to the test.
type Store struct{}

func (s *Store) Append(
	_ context.Context,
	_ string,
	_ int64,
	_ []byte,
) error {
	return nil
}

func (s *Store) ReadFrom(
	_ context.Context,
	_ string,
	_ int64,
) ([][]byte, error) {
	return nil, nil
}

func (s *Store) ReadRange(
	_ context.Context,
	_ string,
	_ int64,
	_ int64,
) ([][]byte, error) {
	return nil, nil
}

func (s *Store) Count(_ context.Context, _ string, _ int64) (int64, error) {
	return 0, nil
}

func (s *Store) Delete(_ context.Context, _ string) error {
	return nil
}

// ErrStore is a Store implementation that returns Err for every operation.
type ErrStore struct{ Err error }

func (e *ErrStore) Append(_ context.Context, _ string, _ int64, _ []byte) error {
	return e.Err
}

func (e *ErrStore) ReadFrom(_ context.Context, _ string, _ int64) ([][]byte, error) {
	return nil, e.Err
}

func (e *ErrStore) ReadRange(_ context.Context, _ string, _ int64, _ int64) ([][]byte, error) {
	return nil, e.Err
}

func (e *ErrStore) Count(_ context.Context, _ string, _ int64) (int64, error) {
	return 0, e.Err
}

func (e *ErrStore) Delete(_ context.Context, _ string) error {
	return e.Err
}

// CorruptBlobStore is a Store implementation that returns a single
// unparseable blob from reads without returning an error.
// Used to trigger deserialize/unmarshal error paths in tests.
type CorruptBlobStore struct{}

func (c *CorruptBlobStore) Append(_ context.Context, _ string, _ int64, _ []byte) error {
	return nil
}

func (c *CorruptBlobStore) ReadFrom(_ context.Context, _ string, _ int64) ([][]byte, error) {
	return [][]byte{[]byte("!!!invalid")}, nil
}

func (c *CorruptBlobStore) ReadRange(_ context.Context, _ string, _ int64, _ int64) ([][]byte, error) {
	return [][]byte{[]byte("!!!invalid")}, nil
}

func (c *CorruptBlobStore) Count(_ context.Context, _ string, _ int64) (int64, error) {
	return 1, nil
}

func (c *CorruptBlobStore) Delete(_ context.Context, _ string) error {
	return nil
}
