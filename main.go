package main

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	indexer "github.com/RangerMauve/ipld-prolly-indexer/indexer"
	car "github.com/ipld/go-car/v2"
	carBlockstore "github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime"

	cid "github.com/ipfs/go-cid"
	dagjson "github.com/ipld/go-ipld-prime/codec/dagjson"
	datamodel "github.com/ipld/go-ipld-prime/datamodel"
	qp "github.com/ipld/go-ipld-prime/fluent/qp"
	basicnode "github.com/ipld/go-ipld-prime/node/basicnode"

	"github.com/urfave/cli/v2"
)

// This is the default CID that gets
const EMPTY_DB_ROOT = "bafyrefczokuljxpuzx3ivzun5p5jdfnfdj3qzqq"

func main() {
	app := &cli.App{
		Name:  "ipti",
		Usage: "Explore databases in the IPLD Prolly Tree Indexer format",
		Commands: []*cli.Command{
			{
				Name:  "dump",
				Usage: "dump a collection into a csv file",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "input",
						Aliases: []string{"i"},
						Value:   "./car.db",
						Usage:   "Path to database file to read.",
					},
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Value:   "",
						Usage:   "Path to CSV file to wite. Omit to output to STDOUT",
					},
					&cli.StringFlag{
						Name:    "collection",
						Aliases: []string{"c"},
						Value:   "default",
						Usage:   "Name of database collection to save into",
					},
					&cli.StringFlag{
						Name:  "id",
						Value: "",
						Usage: "Specify a column to output the record ID under. Omit to skip.",
					},
				},
				Action: func(ctx *cli.Context) error {
					input := ctx.String("input")
					output := ctx.String("output")
					collectionName := ctx.String("collection")
					id := ctx.String("id")

					if len(output) != 0 {
						file, err := os.Create(output)
						if err != nil {
							return err
						}
						defer file.Close()
						// Create a writer from the file
						return Dump(file, input, collectionName, id)
					} else {
						return Dump(os.Stdout, input, collectionName, id)

					}
				},
			}, {
				Name:  "ingest",
				Usage: "ingest a collection into a prolly tree from a csv file",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "output",
						Aliases: []string{"o"},
						Value:   "./db.prolly.car",
						Usage:   "Path to database file to write.",
					},
					&cli.StringFlag{
						Name:    "input",
						Aliases: []string{"i"},
						Value:   "",
						Usage:   "Path to CSV file to read. Omit to read from STDIN",
					},
					&cli.StringFlag{
						Name:    "collection",
						Aliases: []string{"c"},
						Value:   "default",
						Usage:   "Name of database collection to save into",
					},
					&cli.BoolFlag{
						Name:  "add-row-index",
						Value: false,
						Usage: "Add an `index` column with the 0-indexed row count",
					},
				},
				Action: func(ctx *cli.Context) error {
					input := ctx.String("input")
					output := ctx.String("output")
					collectionName := ctx.String("collection")
					addRowIndex := ctx.Bool("add-row-index")

					if len(input) != 0 {
						file, err := os.Open(input)
						if err != nil {
							return err
						}
						defer file.Close()
						// Create a writer from the file
						return Ingest(output, file, collectionName, addRowIndex)
					} else {
						return Ingest(output, os.Stdin, collectionName, addRowIndex)
					}
				},
			},
			{
				Name:  "root",
				Usage: "Get the root CID from a CAR file",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "input",
						Aliases: []string{"i"},
						Value:   "./db.prolly.car",
						Usage:   "Path to database file to read.",
					},
				},
				Action: func(ctx *cli.Context) error {
					input := ctx.String("input")
					return PrintRoot(input)
				},
			},
			{
				Name:  "list",
				Usage: "List collections in a CAR",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "input",
						Aliases: []string{"i"},
						Value:   "./db.prolly.car",
						Usage:   "Path to database file to read.",
					},
				},
				Action: func(ctx *cli.Context) error {
					input := ctx.String("input")
					return ListCollections(input)
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func ListCollections(input string) error {
	ctx := context.Background()

	db, err := indexer.ImportFromFile(input)

	if err != nil {
		return err
	}

	collections, err := db.ListCollections(ctx)

	if err != nil {
		return err
	}

	for _, item := range collections {
		fmt.Println(item)
	}

	return nil
}

func PrintRoot(input string) error {
	reader, err := car.OpenReader(input)

	if err != nil {
		return err
	}

	defer reader.Close()

	roots, err := reader.Roots()

	if err != nil {
		return err
	}

	for _, root := range roots {
		fmt.Println(root)
	}

	return nil
}

func Dump(
	output io.Writer,
	input string,
	collectionName string,
	id string,
) error {
	appendId := len(id) != 0
	ctx := context.Background()

	db, err := indexer.ImportFromFile(input)

	if err != nil {
		return err
	}

	collection, err := db.Collection(ctx, collectionName)

	if err != nil {
		return err
	}

	records, err := collection.Search(ctx, indexer.Query{})

	if err != nil {
		return err
	}

	columns := []string{}
	rowSize := 0
	first := true

	writer := csv.NewWriter(output)

	defer writer.Flush()

	for record := range records {
		if first {
			first = false
			columns, err = mapKeys(record.Data)

			if err != nil {
				return err
			}
			headers := []string{}
			headers = append(headers, columns...)
			rowSize = len(columns)
			if appendId {
				rowSize += 1
				headers = append(headers, id)
			}
			err = writer.Write(headers)

			if err != nil {
				return err
			}
		}

		row := make([]string, rowSize)

		for column := range columns {
			key := columns[column]
			valueNode, err := record.Data.LookupByString(key)

			if err != nil {
				return err
			}
			value, err := valueAsString(valueNode)

			if err != nil {
				return err
			}

			row[column] = value

		}
		if appendId {
			row[rowSize-1] = base64.RawURLEncoding.EncodeToString(record.Id)
		}
		//fmt.Println(row)
		err := writer.Write(row)

		if err != nil {
			return err
		}
	}

	return nil
}

func valueAsString(valueNode ipld.Node) (string, error) {
	kind := valueNode.Kind()
	if kind == datamodel.Kind_String {

		return valueNode.AsString()

	} else if kind == datamodel.Kind_Int {

		value, err := valueNode.AsInt()

		if err != nil {
			return "", err
		}

		return strconv.Itoa(int(value)), nil
	} else {
		return "", fmt.Errorf("unable to convert IPLD nodes to csv string %q", kind)
	}
}

func mapKeys(node ipld.Node) ([]string, error) {
	iterator := node.MapIterator()
	keys := []string{}

	for !iterator.Done() {
		keyNode, _, err := iterator.Next()

		if err != nil {
			return nil, err
		}
		key, err := keyNode.AsString()

		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	return keys, nil

}

func Ingest(
	output string,
	reader io.Reader,
	collectionName string,
	addRowIndex bool,
) error {

	ctx := context.Background()

	defaultCid, err := cid.Decode(EMPTY_DB_ROOT)

	if err != nil {
		return err
	}

	// Set a default CID so the CAR header has space for final CID
	blockstore, err := carBlockstore.OpenReadWrite(output, []cid.Cid{defaultCid})

	if err != nil {
		return err
	}

	db, err := indexer.NewDatabaseFromBlockStore(ctx, blockstore)

	if err != nil {
		return err
	}

	collection, err := db.Collection(ctx, collectionName)

	if err != nil {
		return err
	}

	if addRowIndex {
		_, err = collection.CreateIndex(ctx, "index")

		if err != nil {
			return err
		}
	}

	err = ingestCSV(ctx, reader, collection, addRowIndex)

	if err != nil {
		return err
	}

	err = db.ApplyChanges(ctx)

	if err != nil {
		return err
	}
	finalCid := db.RootCid()

	err = blockstore.Finalize()

	if err != nil {
		return err
	}

	err = car.ReplaceRootsInFile(output, []cid.Cid{finalCid})

	if err != nil {
		return err
	}

	fmt.Println(finalCid)

	return nil
}

func ingestCSV(ctx context.Context, source io.Reader, collection *indexer.Collection, addRowIndex bool) error {
	reader := csv.NewReader(source)

	headers, err := reader.Read()

	numFields := int64(len(headers))

	if addRowIndex {
		numFields += 1
	}

	if err != nil {
		return err
	}

	index := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		node, err := qp.BuildMap(basicnode.Prototype.Any, numFields, func(ma datamodel.MapAssembler) {
			if addRowIndex {
				qp.MapEntry(ma, "index", qp.Int(int64(index)))
			}
			for fieldIndex, fieldValue := range record {
				nb := basicnode.Prototype__Any{}.NewBuilder()
				err := dagjson.Decode(nb, strings.NewReader(fieldValue))

				// If it wasn't json, it's just a string
				if err != nil {
					qp.MapEntry(ma, headers[fieldIndex], qp.String(fieldValue))
				} else {
					value := nb.Build()
					qp.MapEntry(ma, headers[fieldIndex], qp.Node(value))
				}
			}
		})

		if err != nil {
			return err
		}

		err = collection.Insert(ctx, node)

		if err != nil {
			return err
		}

		index++
	}

	return nil
}
