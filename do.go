package transformer

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"fmt"
	"google.golang.org/api/iterator"
	"sync"
)

// New creates a new transformer Doer
func New(cli *datastore.Client, query *datastore.Query, opts ...Option) (*Doer, error) {
	d := &Doer{
		cli:     cli,
		query:   query,
		workers: 1,
		tasks:   make(chan *Entity),
	}
	for _, opt := range opts {
		if err := opt(d); err != nil {
			return nil, err
		}
	}
	return d, nil
}

// Doer runs the query and sends to the workers the entities
// to transform and save.
type Doer struct {
	cli          *datastore.Client
	query        *datastore.Query
	transformers []Transformer
	workers      int
	wg           sync.WaitGroup
	tasks        chan *Entity
}

// Apply adds Transformer to be applied to an Entity
func (d *Doer) Apply(t ...Transformer) {
	d.transformers = append(d.transformers, t...)
}

// Do queries the entities and sends to the workers
func (d *Doer) Do(ctx context.Context) error {

	defer func() {
		close(d.tasks)
		d.wg.Wait()
	}()

	// Init workers
	d.wg.Add(d.workers)
	for i := 0; i < d.workers; i++ {
		w := &worker{
			wg:           &d.wg,
			cli:          d.cli,
			tasks:        d.tasks,
			transformers: d.transformers,
		}
		w.Run(ctx)
	}

	// Iterate entities
	it := d.cli.Run(ctx, d.query)
	for {

		var entity = newEntity()
		_, err := it.Next(entity)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return err
		}

		// Send to workers
		select {
		case d.tasks <- entity:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// worker applies transformers to the entities and save it
type worker struct {
	wg           *sync.WaitGroup
	cli          *datastore.Client
	tasks        <-chan *Entity
	transformers []Transformer
}

func (w *worker) Run(ctx context.Context) {

	go func() {

		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case task, ok := <-w.tasks:

				if !ok {
					return
				}

				for _, t := range w.transformers {
					t.Transform(task)
				}
				if _, err := w.cli.Put(ctx, task.key, task); err != nil {
					// TODO send to error channel
					fmt.Println(err.Error())
				}
			}
		}

	}()
}
