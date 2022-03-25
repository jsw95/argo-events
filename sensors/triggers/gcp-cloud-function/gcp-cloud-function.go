package gcp_cloud_function

import (
	"context"
	"encoding/json"
	"os"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/argoproj/argo-events/common"
	"github.com/argoproj/argo-events/common/logging"
	apicommon "github.com/argoproj/argo-events/pkg/apis/common"
	"github.com/argoproj/argo-events/pkg/apis/sensor/v1alpha1"
	"github.com/argoproj/argo-events/sensors/triggers"
	"google.golang.org/api/cloudfunctions/v1"
	"google.golang.org/api/option"
)

// GCPCloudFunctionTrigger describes the trigger to send messages to an Event Hub
type GCPCloudFunctionTrigger struct {
	// Sensor object
	Sensor *v1alpha1.Sensor
	// Trigger reference
	Trigger *v1alpha1.Trigger
	// Service refers to GCP Cloud Function Service
	Service *cloudfunctions.Service
	// Logger to log stuff
	Logger *zap.SugaredLogger
}

func NewGCPCloudFunctionTrigger(gcpClients map[string]*cloudfunctions.Service, sensor *v1alpha1.Sensor, trigger *v1alpha1.Trigger, logger *zap.SugaredLogger) (*GCPCloudFunctionTrigger, error) {
	gcptrigger := trigger.Template.GCPCloudFunction

	gcpClient, ok := gcpClients[trigger.Template.Name]
	if !ok {
		credentialsPath, err := common.GetSecretFromVolume(gcptrigger.CredentialsPath)
		if err != nil {
			return nil, errors.Wrap(err, "can not find service account path key")
		}
		if _, err := os.Stat(credentialsPath); errors.Is(err, os.ErrNotExist) {
			return nil, errors.Wrap(err, "can not find service account file from CredentialsPath")
		}

		opts := []option.ClientOption{
			option.WithCredentialsFile(credentialsPath),
		}
		gcpClient, err := cloudfunctions.NewService(context.Background(), opts...)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create a GCP service")
		}
		gcpClients[trigger.Template.Name] = gcpClient
	}

	return &GCPCloudFunctionTrigger{
		Service: gcpClient,
		Sensor:  sensor,
		Trigger: trigger,
		Logger:  logger.With(logging.LabelTriggerType, apicommon.GCPFunctionTrigger),
	}, nil
}

// GetTriggerType returns the type of the trigger
func (t *GCPCloudFunctionTrigger) GetTriggerType() apicommon.TriggerType {
	return apicommon.GCPFunctionTrigger
}

// FetchResource fetches the trigger resource
func (t *GCPCloudFunctionTrigger) FetchResource(context.Context) (interface{}, error) {
	return t.Trigger.Template.GCPCloudFunction, nil
}

// ApplyResourceParameters applies parameters to the trigger resource
func (t *GCPCloudFunctionTrigger) ApplyResourceParameters(events map[string]*v1alpha1.Event, resource interface{}) (interface{}, error) {
	resourceBytes, err := json.Marshal(resource)
	if err != nil {
		return nil, errors.Wrap(err, "failed to marshal the GCP cloud function trigger resource")
	}
	parameters := t.Trigger.Template.GCPCloudFunction.Parameters
	if parameters != nil {
		updatedResourceBytes, err := triggers.ApplyParams(resourceBytes, t.Trigger.Template.GCPCloudFunction.Parameters, events)
		if err != nil {
			return nil, err
		}
		var ht *v1alpha1.GCPCloudFunctionTrigger
		if err := json.Unmarshal(updatedResourceBytes, &ht); err != nil {
			return nil, errors.Wrap(err, "failed to unmarshal the updated GCP cloud function trigger resource after applying resource parameters")
		}
		return ht, nil
	}
	return resource, nil
}

// Execute executes the trigger
func (t *GCPCloudFunctionTrigger) Execute(ctx context.Context, events map[string]*v1alpha1.Event, resource interface{}) (interface{}, error) {
	trigger, ok := resource.(*v1alpha1.GCPCloudFunctionTrigger)
	if !ok {
		return nil, errors.New("failed to interpret the trigger resource")
	}

	if trigger.Payload == nil {
		return nil, errors.New("payload parameters are not specified")
	}

	payload, err := triggers.ConstructPayload(events, trigger.Payload)
	if err != nil {
		return nil, err
	}
	request := cloudfunctions.CallFunctionRequest{Data: string(payload)}
	response, err := t.Service.Projects.Locations.Functions.Call(trigger.FunctionName, &request).Do()
	if err != nil {
		return nil, err
	}

	return response, nil
}

// ApplyPolicy applies the policy on the trigger execution response
func (t *GCPCloudFunctionTrigger) ApplyPolicy(ctx context.Context, resource interface{}) error {
	return nil
}
