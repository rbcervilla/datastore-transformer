package transformer

import "fmt"

// Option is a functional option for configuring the Doer
type Option func(d *Doer) error

// WithConcurrency function to set numer of workers running concurrently
func WithConcurrency(c int) Option {
	return func(d *Doer) error {
		if c <= 0 {
			return fmt.Errorf("invalid concurrency option %d", c)
		}
		d.workers = c
		return nil
	}
}
