package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/spf13/cobra"
	"gocloud.dev/blob"
	"gocloud.dev/blob/azureblob"
)

const (
	accountName azureblob.AccountName = "ENTER_YOUR_ACCOUNT_NAME"
	accountKey  azureblob.AccountKey  = "ENTER_YOUR_KEY"
)

var (
	// Global variables
	ctx        context.Context
	credential *azblob.SharedKeyCredential
	pline      pipeline.Pipeline

	// Flags
	containerName string
	blobKey       string
	blobValue     string
	blobPrefix    string

	// Commands
	rootCmd = &cobra.Command{
		Use:   "azure",
		Short: "Interact with azure using the azure CLI",
	}

	createContainerCmd = &cobra.Command{
		Use:   "create-container",
		Short: "Create an azure container",
		Run: func(cmd *cobra.Command, args []string) {
			// From the Azure portal, get your storage account blob service URL endpoint.
			URL, _ := url.Parse(
				fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

			// Create a ContainerURL object that wraps the container URL and a request
			// pipeline to make requests.
			containerURL := azblob.NewContainerURL(*URL, pline)
			fmt.Printf("Creating a container named %q\n", containerName)
			_, err := containerURL.Create(ctx, azblob.Metadata{}, azblob.PublicAccessNone)
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Successfully created container %q\n", containerName)
		},
	}

	deleteContainerCmd = &cobra.Command{
		Use:   "delete-container",
		Short: "Delete an azure container",
		Run: func(cmd *cobra.Command, args []string) {
			// From the Azure portal, get your storage account blob service URL endpoint.
			URL, _ := url.Parse(
				fmt.Sprintf("https://%s.blob.core.windows.net/%s", accountName, containerName))

			// Create a ContainerURL object that wraps the container URL and a request
			// pipeline to make requests.
			containerURL := azblob.NewContainerURL(*URL, pline)
			fmt.Printf("Deleting a container named %q\n", containerName)
			_, err := containerURL.Delete(ctx, azblob.ContainerAccessConditions{})
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Successfully deleted container %q\n", containerName)
		},
	}

	writeCmd = &cobra.Command{
		Use:   "write",
		Short: "Write to a blob",
		Run: func(cmd *cobra.Command, args []string) {
			// Check if valid flags
			if blobKey == "" {
				log.Fatal(fmt.Errorf(`flag "--blob-key" should be set`))
			}

			if blobValue == "" {
				log.Fatal(fmt.Errorf(`flag "--blob-value" should be set`))
			}

			// Create a *blob.Bucket.
			// The credential Option is required if you're going to use blob.SignedURL.
			bucket, err := azureblob.OpenBucket(ctx, pline, accountName, containerName,
				&azureblob.Options{Credential: credential})
			if err != nil {
				log.Fatal(err)
			}
			defer bucket.Close()

			// Write
			w, err := bucket.NewWriter(ctx, blobKey, nil)
			if err != nil {
				log.Fatal(err)
			}

			_, err = fmt.Fprintln(w, blobValue)
			if err != nil {
				log.Fatal(err)
			}

			err = w.Close()
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Successfully written %q to %q\n", blobValue, blobKey)
		},
	}

	readCmd = &cobra.Command{
		Use:   "read",
		Short: "Read from a blob",
		Run: func(cmd *cobra.Command, args []string) {
			// Check if valid flags
			if blobKey == "" {
				log.Fatal(fmt.Errorf(`flag "--blob-key" should be set`))
			}

			// Create a *blob.Bucket.
			// The credential Option is required if you're going to use blob.SignedURL.
			bucket, err := azureblob.OpenBucket(ctx, pline, accountName, containerName,
				&azureblob.Options{Credential: credential})
			if err != nil {
				log.Fatal(err)
			}
			defer bucket.Close()

			// Open the key blobKey for reading with the default options.
			r, err := bucket.NewReader(ctx, blobKey, nil)
			if err != nil {
				log.Fatal(err)
			}
			defer r.Close()

			// Readers also have a limited view of the blob's metadata.
			fmt.Println("Content-Type:", r.ContentType())
			fmt.Println()
			// Copy from the reader to stdout.
			if _, err := io.Copy(os.Stdout, r); err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Successfully read from %q\n", blobKey)
		},
	}

	listCmd = &cobra.Command{
		Use:   "list",
		Short: "List from a blob with (or without) a prefix",
		Run: func(cmd *cobra.Command, args []string) {
			// Create a *blob.Bucket.
			// The credential Option is required if you're going to use blob.SignedURL.
			bucket, err := azureblob.OpenBucket(ctx, pline, accountName, containerName,
				&azureblob.Options{Credential: credential})
			if err != nil {
				log.Fatal(err)
			}
			defer bucket.Close()

			// Create a prefixed bucket
			bucket = blob.PrefixedBucket(bucket, blobPrefix)
			defer bucket.Close()

			// list lists files in b starting with prefix. It uses the delimiter "/",
			// and recurses into "directories", adding 2 spaces to indent each time.
			// It will list the blobs created above because fileblob is strongly
			// consistent, but is not guaranteed to work on all services.
			var list func(context.Context, *blob.Bucket, string, string)
			list = func(ctx context.Context, b *blob.Bucket, prefix, indent string) {
				iter := b.List(&blob.ListOptions{
					Delimiter: "/",
					Prefix:    prefix,
				})
				for {
					obj, err := iter.Next(ctx)
					if err == io.EOF {
						break
					}
					if err != nil {
						log.Fatal(err)
					}
					fmt.Printf("%s%s\n", indent, obj.Key)
					if obj.IsDir {
						list(ctx, b, obj.Key, indent+"  ")
					}
				}
			}
			list(ctx, bucket, "", "")

			fmt.Printf("Successfully listed from %q\n", blobPrefix)
		},
	}
)

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}

func init() {
	// Add flags
	rootCmd.PersistentFlags().StringVar(&containerName, "container-name", "default-container-name", "indicate a name of the container")
	writeCmd.PersistentFlags().StringVar(&blobKey, "blob-key", "", "indicate a blob key for writing")
	writeCmd.PersistentFlags().StringVar(&blobValue, "blob-value", "", "indicate a value you want to write to a given blob-key")
	readCmd.PersistentFlags().StringVar(&blobKey, "blob-key", "", "indicate a blob key for writing")
	listCmd.PersistentFlags().StringVar(&blobPrefix, "blob-prefix", "", "indicate a blob prefix to read from subdirectories")

	// Add commands
	rootCmd.AddCommand(createContainerCmd)
	rootCmd.AddCommand(deleteContainerCmd)
	rootCmd.AddCommand(writeCmd)
	rootCmd.AddCommand(readCmd)
	rootCmd.AddCommand(listCmd)

	// Init azure
	// Create a credentials object.
	ctx = context.Background()
	credential, err := azureblob.NewCredential(accountName, accountKey)
	if err != nil {
		log.Fatal(err)
	}

	// Create a Pipeline, using whatever PipelineOptions you need.
	pline = azureblob.NewPipeline(credential, azblob.PipelineOptions{})
}

func main() {
	Execute()
}
