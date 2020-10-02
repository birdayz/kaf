package gnomock

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"go.uber.org/zap"
)

const localhostAddr = "127.0.0.1"
const defaultStopTimeout = time.Second * 1
const duplicateContainerPattern = `Conflict. The container name "(?:.+?)" is already in use by container "(\w+)". You have to remove \(or rename\) that container to be able to reuse that name.` // nolint:lll

type docker struct {
	client *client.Client
	log    *zap.SugaredLogger
}

func (g *g) dockerConnect() (*docker, error) {
	g.log.Info("connecting to docker engine")

	cli, err := client.NewEnvClient()
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrEnvClient, err)
	}

	g.log.Info("connected to docker engine")

	return &docker{cli, g.log}, nil
}

func (d *docker) pullImage(ctx context.Context, image string) error {
	d.log.Info("pulling image")

	reader, err := d.client.ImagePull(ctx, image, types.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("can't pull image: %w", err)
	}

	defer func() {
		closeErr := reader.Close()

		if err == nil {
			err = closeErr
		}
	}()

	_, err = ioutil.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("can't read server output: %w", err)
	}

	d.log.Info("image pulled")

	return nil
}

func (d *docker) startContainer(ctx context.Context, image string, ports NamedPorts, cfg *Options) (*Container, error) {
	d.log.Info("starting container")

	resp, err := d.createContainer(ctx, image, ports, cfg)
	if err != nil {
		return nil, fmt.Errorf("can't create container: %w", err)
	}

	err = d.client.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{})
	if err != nil {
		return nil, fmt.Errorf("can't start container %s: %w", resp.ID, err)
	}

	containerJSON, err := d.client.ContainerInspect(ctx, resp.ID)
	if err != nil {
		return nil, fmt.Errorf("can't inspect container %s: %w", resp.ID, err)
	}

	boundNamedPorts, err := d.boundNamedPorts(containerJSON, ports)
	if err != nil {
		return nil, fmt.Errorf("can't find bound ports: %w", err)
	}

	container := &Container{
		ID:      containerJSON.ID,
		Host:    localhostAddr,
		Ports:   boundNamedPorts,
		gateway: containerJSON.NetworkSettings.Gateway,
	}

	d.log.Infow("container started", "container", container)

	return container, nil
}

func (d *docker) exposedPorts(namedPorts NamedPorts) nat.PortSet {
	exposedPorts := make(nat.PortSet)

	for _, port := range namedPorts {
		containerPort := fmt.Sprintf("%d/%s", port.Port, port.Protocol)
		exposedPorts[nat.Port(containerPort)] = struct{}{}
	}

	return exposedPorts
}

func (d *docker) portBindings(exposedPorts nat.PortSet, ports NamedPorts) nat.PortMap {
	portBindings := make(nat.PortMap)

	// for the container to be accessible from another container, it cannot
	// listen on 127.0.0.1 as it will be accessed by gateway address (e.g
	// 172.17.0.1), so its port should be exposed everywhere
	hostAddr := localhostAddr
	if isInDocker() {
		hostAddr = "0.0.0.0"
	}

	for port := range exposedPorts {
		binding := nat.PortBinding{
			HostIP: hostAddr,
		}

		if pName, err := ports.Find(port.Proto(), port.Int()); err == nil {
			namedPort := ports.Get(pName)
			if namedPort.HostPort > 0 {
				binding.HostPort = strconv.Itoa(namedPort.HostPort)
			}
		}

		portBindings[port] = []nat.PortBinding{binding}
	}

	return portBindings
}

func (d *docker) createContainer(ctx context.Context, image string, ports NamedPorts, cfg *Options) (*container.ContainerCreateCreatedBody, error) { // nolint:lll
	exposedPorts := d.exposedPorts(ports)
	containerConfig := &container.Config{
		Image:        image,
		ExposedPorts: exposedPorts,
		Env:          cfg.Env,
	}
	portBindings := d.portBindings(exposedPorts, ports)
	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
		AutoRemove:   true,
	}

	resp, err := d.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, cfg.ContainerName)
	if err == nil {
		return &resp, nil
	}

	rxp, rxpErr := regexp.Compile(duplicateContainerPattern)
	if rxpErr != nil {
		return nil, fmt.Errorf("can't find conflicting container id: %w", err)
	}

	matches := rxp.FindStringSubmatch(err.Error())
	if len(matches) == 2 {
		d.log.Infow("duplicate container found, stopping", "container", matches[1])

		err = d.client.ContainerRemove(ctx, matches[1], types.ContainerRemoveOptions{
			Force: true,
		})
		if err != nil {
			return nil, fmt.Errorf("can't remove existing container: %w", err)
		}

		resp, err = d.client.ContainerCreate(ctx, containerConfig, hostConfig, nil, cfg.ContainerName)
	}

	return &resp, err
}

func (d *docker) boundNamedPorts(json types.ContainerJSON, namedPorts NamedPorts) (NamedPorts, error) {
	boundNamedPorts := make(NamedPorts)

	for containerPort, bindings := range json.NetworkSettings.Ports {
		if len(bindings) == 0 {
			continue
		}

		hostPortNum, err := strconv.Atoi(bindings[0].HostPort)
		if err != nil {
			return nil, err
		}

		portName, err := namedPorts.Find(containerPort.Proto(), containerPort.Int())
		if err != nil {
			return nil, err
		}

		boundNamedPorts[portName] = Port{
			Protocol: containerPort.Proto(),
			Port:     hostPortNum,
		}
	}

	return boundNamedPorts, nil
}

func (d *docker) readLogs(ctx context.Context, id string) (io.ReadCloser, error) {
	d.log.Info("starting container logs forwarder")

	logsOptions := types.ContainerLogsOptions{
		ShowStderr: true, ShowStdout: true, Follow: true,
	}

	rc, err := d.client.ContainerLogs(ctx, id, logsOptions)
	if err != nil {
		return nil, fmt.Errorf("can't read logs: %w", err)
	}

	d.log.Info("container logs forwarder ready")

	return rc, nil
}

func (d *docker) stopContainer(ctx context.Context, id string) error {
	stopTimeout := defaultStopTimeout

	err := d.client.ContainerStop(ctx, id, &stopTimeout)
	if err != nil {
		return fmt.Errorf("can't stop container %s: %w", id, err)
	}

	return nil
}
