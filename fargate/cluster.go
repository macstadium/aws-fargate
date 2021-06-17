package fargate

import (
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
)

const (
	clusterFailureReasonMissing = "MISSING"
)

// ClusterConfig contains a Fargate cluster's configurable parameters.
type ClusterConfig struct {
	Region                  string
	Name                    string
	NodeName                string
	Subnets                 []string
	SecurityGroups          []string
	AssignPublicIPv4Address bool
	ExecutionRoleArn        string
	CloudWatchLogGroupName  string
	PlatformVersion         string
}

// Cluster represents a Fargate cluster.
type Cluster struct {
	region                  string
	name                    string
	nodeName                string
	arn                     string
	subnets                 []string
	securityGroups          []string
	assignPublicIPv4Address bool
	executionRoleArn        string
	cloudWatchLogGroupName  string
	platformVersion         string
	pods                    map[string]*Pod
	sync.RWMutex
}

// NewCluster creates a new Cluster object.
func NewCluster(config *ClusterConfig) (*Cluster, error) {
	var err error

	// Cluster name cannot contain '_' as it is used as a separator in task tags.
	if strings.Contains(config.Name, "_") {
		return nil, fmt.Errorf("cluster name should not contain the '_' character")
	}

	// Check if Fargate is available in the given region.
	if !FargateRegions.Include(config.Region) {
		return nil, fmt.Errorf("Fargate is not available in region %s", config.Region)
	}

	// Create the client to the regional Fargate service.
	client, err = newClient(config.Region)
	if err != nil {
		return nil, fmt.Errorf("failed to create Fargate client: %v", err)
	}

	// Initialize the cluster.
	cluster := &Cluster{
		region:                  config.Region,
		name:                    config.Name,
		nodeName:                config.NodeName,
		subnets:                 config.Subnets,
		securityGroups:          config.SecurityGroups,
		assignPublicIPv4Address: config.AssignPublicIPv4Address,
		executionRoleArn:        config.ExecutionRoleArn,
		cloudWatchLogGroupName:  config.CloudWatchLogGroupName,
		platformVersion:         config.PlatformVersion,
		pods:                    make(map[string]*Pod),
	}

	// If a node name is not specified, use the Fargate cluster name.
	if cluster.nodeName == "" {
		cluster.nodeName = cluster.name
	}

	// Check if the cluster already exists.
	err = cluster.describe()
	if err != nil && !strings.Contains(err.Error(), clusterFailureReasonMissing) {
		return nil, err
	}

	// If not, try to create it.
	// This might fail if the principal doesn't have the necessary permission.
	if cluster.arn == "" {
		err = cluster.create()
		if err != nil {
			return nil, err
		}
	}

	return cluster, nil
}

// Create creates a new Fargate cluster.
func (c *Cluster) create() error {
	return nil
}

// Describe loads information from an existing Fargate cluster.
func (c *Cluster) describe() error {
	return nil
}

// GetPod returns a Kubernetes pod deployed on this cluster.
func (c *Cluster) GetPod(namespace string, name string) (*Pod, error) {
	c.RLock()
	defer c.RUnlock()

	tag := buildTaskDefinitionTag(c.name, namespace, name)
	pod, ok := c.pods[tag]
	if !ok {
		return nil, errdefs.NotFoundf("pod %s/%s is not found", namespace, name)
	}

	return pod, nil
}

// GetPods returns all Kubernetes pods deployed on this cluster.
func (c *Cluster) GetPods() ([]*Pod, error) {
	c.RLock()
	defer c.RUnlock()

	pods := make([]*Pod, 0, len(c.pods))

	for _, pod := range c.pods {
		pods = append(pods, pod)
	}

	return pods, nil
}

// InsertPod inserts a Kubernetes pod to this cluster.
func (c *Cluster) InsertPod(pod *Pod, tag string) {
	c.Lock()
	defer c.Unlock()

	c.pods[tag] = pod
}

// RemovePod removes a Kubernetes pod from this cluster.
func (c *Cluster) RemovePod(tag string) {
	c.Lock()
	defer c.Unlock()

	delete(c.pods, tag)
}

// GetContainerLogs returns the logs of a container from this cluster.
func (c *Cluster) GetContainerLogs(namespace, podName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	if c.cloudWatchLogGroupName == "" {
		return nil, fmt.Errorf("logs not configured, please specify a \"CloudWatchLogGroupName\"")
	}

	prefix := fmt.Sprintf("%s_%s", buildTaskDefinitionTag(c.name, namespace, podName), containerName)
	describeResult, err := client.logsapi.DescribeLogStreams(&cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName:        aws.String(c.cloudWatchLogGroupName),
		LogStreamNamePrefix: aws.String(prefix),
	})
	if err != nil {
		return nil, err
	}

	// Nothing logged yet.
	if len(describeResult.LogStreams) == 0 {
		return nil, nil
	}

	logs := ""

	err = client.logsapi.GetLogEventsPages(&cloudwatchlogs.GetLogEventsInput{
		Limit:         aws.Int64(int64(opts.Tail)),
		LogGroupName:  aws.String(c.cloudWatchLogGroupName),
		LogStreamName: describeResult.LogStreams[0].LogStreamName,
	}, func(page *cloudwatchlogs.GetLogEventsOutput, lastPage bool) bool {
		for _, event := range page.Events {
			logs += *event.Message
			logs += "\n"
		}

		// Due to a issue in the aws-sdk last page is never true, but the we can stop
		// as soon as no further results are returned.
		// See https://github.com/aws/aws-sdk-ruby/pull/730.
		return len(page.Events) > 0
	})

	if err != nil {
		return nil, err
	}

	return ioutil.NopCloser(strings.NewReader(logs)), nil
}
