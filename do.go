package transformer

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"fmt"
	"google.golang.org/api/iterator"
	"sync"
)

// New creates a new Doer
func New(cli *datastore.Client, query *datastore.Query) *Doer {
	return &Doer{
		cli:      cli,
		query:    query,
		nWorkers: 5, // TODO to config
	}
}

// Doer runs the query and sends to the workers the entities
// to transform and save.
type Doer struct {
	cli          *datastore.Client
	query        *datastore.Query
	nWorkers     int
	wg           sync.WaitGroup
	transformers []Transformer
	jobs         chan *Entity
}

// Apply adds Transformer to be applied to an Entity
func (d *Doer) Apply(t ...Transformer) {
	d.transformers = append(d.transformers, t...)
}

// Do queries the entities and sends to the workers
func (d *Doer) Do(ctx context.Context) error {

	d.jobs = make(chan *Entity)

	defer func() {
		close(d.jobs)
		d.wg.Wait()
	}()

	// Init workers
	d.wg.Add(d.nWorkers)
	for i := 0; i < d.nWorkers; i++ {
		w := &worker{
			wg:           &d.wg,
			cli:          d.cli,
			jobs:         d.jobs,
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
		case d.jobs <- entity:
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
	jobs         <-chan *Entity
	transformers []Transformer
}

func (w *worker) Run(ctx context.Context) {

	go func() {

		defer w.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case job, ok := <-w.jobs:

				if !ok {
					return
				}

				for _, t := range w.transformers {
					t.Transform(job)
				}
				if _, err := w.cli.Put(ctx, job.key, job); err != nil {
					// TODO send to error channel
					fmt.Println(err.Error())
				}
			}
		}

	}()
}
