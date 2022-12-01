package transformer

import (
	"cloud.google.com/go/datastore"
	"context"
	"errors"
	"google.golang.org/api/iterator"
	"sync"
	"time"
)

// New creates a new transformer Doer
func New(cli *datastore.Client, query *datastore.Query, opts ...Option) (*Doer, error) {
	d := &Doer{
		cli:       cli,
		query:     query,
		workers:   1,
		tasks:     make(chan *Entity),
		failOnErr: true,
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

	// failOnErr tells if return when a worker error occurs.
	// Default is true
	failOnErr   bool
	notifyErrCh []chan error
}

// Apply adds Transformer to be applied to an Entity
func (d *Doer) Apply(t ...Transformer) {
	d.transformers = append(d.transformers, t...)
}

func (d *Doer) NotifyErr() <-chan error {
	ch := make(chan error)
	d.notifyErrCh = append(d.notifyErrCh, ch)
	return ch
}

// Do queries the entities and sends to the workers
func (d *Doer) Do(ctx context.Context) error {

	errNotifier := newErrorNotifier()

	defer func() {
		close(d.tasks)
		d.wg.Wait()
		ctxT, _ := context.WithTimeout(ctx, time.Second*10)
		errNotifier.close(ctxT)
	}()

	// Init workers
	d.wg.Add(d.workers)
	var workersErrCh []<-chan error
	for i := 0; i < d.workers; i++ {
		w := &worker{
			wg:           &d.wg,
			cli:          d.cli,
			tasks:        d.tasks,
			transformers: d.transformers,
		}
		workersErrCh = append(workersErrCh, w.run(ctx))
	}

	// Workers error handling
	var failCh <-chan error
	if d.failOnErr {
		failCh = d.NotifyErr()
	}
	errNotifier.notify(workersErrCh, d.notifyErrCh)

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

		if d.failOnErr {
			select {
			case err := <-failCh:
				return err
			default:
			}
		}

		// Send to workers
		select {
		case <-ctx.Done():
			return ctx.Err()
		case d.tasks <- entity:
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

func (w *worker) run(ctx context.Context) chan error {

	errCh := make(chan error)

	go func() {

		defer close(errCh)
		defer w.wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case task, ok := <-w.tasks:
				if !ok {
					return
				}
				if err := w.process(ctx, task); err != nil {
					select {
					case <-ctx.Done():
						return
					case errCh <- err:
					}
				}
			}
		}
	}()

	return errCh
}

func (w *worker) process(ctx context.Context, task *Entity) error {
	for _, t := range w.transformers {
		if err := t.Transform(task); err != nil {
			return err
		}
	}
	if _, err := w.cli.Put(ctx, task.Key, task); err != nil {
		return err
	}
	return nil
}

type errorNotifier struct {
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

func newErrorNotifier() *errorNotifier {
	return &errorNotifier{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

func (n *errorNotifier) notify(in []<-chan error, out []chan error) {

	n.once.Do(func() {
		go func() {
			errCh := make(chan error)
			var wg sync.WaitGroup
			wg.Add(len(in))

			for i := 0; i < len(in); i++ {
				go func(inCh <-chan error) {
					defer wg.Done()
					for {
						select {
						case <-n.stop:
							return
						case err, ok := <-inCh:
							if !ok {
								return
							}
							select {
							case <-n.stop:
								return
							case errCh <- err:
							}
						}
					}
				}(in[i])
			}

			go func() {
				defer func() {
					for _, o := range out {
						close(o)
					}
					close(n.done)
				}()
				for err := range errCh {
					for i := 0; i < len(out); i++ {
						select {
						case <-n.stop:
							return
						case out[i] <- err:
						}
					}
				}
			}()

			wg.Wait()
			close(errCh)
		}()
	})
}

func (n *errorNotifier) close(ctx context.Context) {
	select {
	case <-ctx.Done():
		close(n.stop)
	case <-n.done:
	}
}
