package server

import (
	"context"
	"io"
	"time"

	aegisv1 "github.com/aegis/aegis/gen/go/aegis/v1"
	"github.com/aegis/aegis/services/control-plane/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer adapts the ControlPlane core into the proto-generated
// ControlPlaneTelemetryServer interface.
type GRPCServer struct {
	aegisv1.UnimplementedControlPlaneTelemetryServer
	cp *ControlPlane
}

// NewGRPCServer returns a gRPC server backed by the given ControlPlane.
func NewGRPCServer(cp *ControlPlane) *GRPCServer {
	return &GRPCServer{cp: cp}
}

// StreamTelemetry implements the bidirectional streaming RPC. The agent
// sends AgentTelemetry messages and receives ControlPlaneDirective responses.
func (s *GRPCServer) StreamTelemetry(stream grpc.BidiStreamingServer[aegisv1.AgentTelemetry, aegisv1.ControlPlaneDirective]) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		sample := telemetryFromProto(msg)

		directive, err := s.cp.Ingest(stream.Context(), sample)
		if err != nil {
			if err == ErrResourceExhausted {
				return status.Error(codes.ResourceExhausted, err.Error())
			}
			return status.Errorf(codes.Internal, "ingest error: %v", err)
		}

		if err := stream.Send(directiveToProto(directive)); err != nil {
			return err
		}
	}
}

// SubmitDiagnostics accepts a diagnostic bundle pushed proactively by an agent.
func (s *GRPCServer) SubmitDiagnostics(_ context.Context, bundle *aegisv1.DiagnosticBundle) (*aegisv1.Ack, error) {
	if bundle.GetWorkerId() == "" || bundle.GetIncidentId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id and incident_id are required")
	}
	return &aegisv1.Ack{Accepted: true, Message: "diagnostics received"}, nil
}

// telemetryFromProto converts a proto AgentTelemetry message to the internal
// TelemetrySample type.
func telemetryFromProto(msg *aegisv1.AgentTelemetry) state.TelemetrySample {
	ts, err := time.Parse(time.RFC3339Nano, msg.GetTimestamp())
	if err != nil {
		ts = time.Now().UTC()
	}
	return state.TelemetrySample{
		WorkerID:             msg.GetWorkerId(),
		Timestamp:            ts,
		GPUUtilization:       msg.GetGpuUtilization(),
		VRAMUsedBytes:        msg.GetVramUsedBytes(),
		VRAMTotalBytes:       msg.GetVramTotalBytes(),
		TemperatureCelsius:   msg.GetTemperatureCelsius(),
		PowerWatts:           msg.GetPowerWatts(),
		ECCErrorCount:        msg.GetEccErrorCount(),
		InferenceLatencyMS:   msg.GetInferenceLatencyMs(),
		LocalQueueDepth:      msg.GetLocalQueueDepth(),
		ModelServerHealthy:   msg.GetModelServerHealthy(),
		SyntheticFailureFlag: msg.GetSyntheticFailure(),
		CorrelationID:        msg.GetCorrelationId(),
	}
}

// directiveToProto converts an internal Directive to a proto ControlPlaneDirective.
func directiveToProto(d Directive) *aegisv1.ControlPlaneDirective {
	return &aegisv1.ControlPlaneDirective{
		DirectiveType: d.Type,
		OwnerHint:     d.OwnerHint,
		Message:       d.Message,
		CorrelationId: d.CorrelationID,
	}
}
