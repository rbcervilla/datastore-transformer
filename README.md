# datastore-transformer
Library to apply transformations over Google Datastore entities

:warning: **This repo is not stable yet**: Be careful!

## Example

Remove field `OldField` and add field `NewField` with value 1 to all entities of kind `KindToTransform`

```go
package main

import (
	"cloud.google.com/go/datastore"
	"context"
	"github.com/rbcervilla/datastore-transformer"
)

func main() {

	ctx := context.Background()
	cli, _ := datastore.NewClient(ctx, "project-id")

	tr := transformer.New(cli, datastore.NewQuery("KindToTransform"))

	tr.Apply(
		transformer.RemoveField("OldField"),
		transformer.SetField("NewField", 1, true))

	tr.Do(ctx)
}
```