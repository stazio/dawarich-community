
import 'package:dawarich/features/tracking/domain/enum/location_precision.dart';

final class TrackerSettings {
  final int userId;
  final bool automaticTracking;
  final int trackingFrequency;
  final LocationPrecision locationPrecision;
  final int minimumPointDistance;
  final int pointsPerBatch;
  final int? batchExpirationMinutes;
  final int statusUpdateInterval;
  final String deviceId;

  const TrackerSettings({
    required this.userId,
    required this.automaticTracking,
    required this.trackingFrequency,
    required this.locationPrecision,
    required this.minimumPointDistance,
    required this.pointsPerBatch,
    this.batchExpirationMinutes,
    required this.statusUpdateInterval,
    required this.deviceId,
  });

  /// Whether batch expiration is enabled (non-null and > 0).
  bool get isBatchExpirationEnabled =>
      batchExpirationMinutes != null && batchExpirationMinutes! > 0;

  /// Whether status update interval is enabled (> 0 seconds).
  bool get isStatusUpdateEnabled => statusUpdateInterval > 0;

  TrackerSettings copyWith({
    bool? automaticTracking,
    int? trackingFrequency,
    LocationPrecision? locationPrecision,
    int? minimumPointDistance,
    int? pointsPerBatch,
    int? Function()? batchExpirationMinutes,
    int? statusUpdateInterval,
    String? deviceId,
  }) {
    return TrackerSettings(
      userId: userId,
      automaticTracking: automaticTracking ?? this.automaticTracking,
      trackingFrequency: trackingFrequency ?? this.trackingFrequency,
      locationPrecision: locationPrecision ?? this.locationPrecision,
      minimumPointDistance: minimumPointDistance ?? this.minimumPointDistance,
      pointsPerBatch: pointsPerBatch ?? this.pointsPerBatch,
      batchExpirationMinutes: batchExpirationMinutes != null
          ? batchExpirationMinutes()
          : this.batchExpirationMinutes,
      statusUpdateInterval: statusUpdateInterval ?? this.statusUpdateInterval,
      deviceId: deviceId ?? this.deviceId,
    );
  }
}