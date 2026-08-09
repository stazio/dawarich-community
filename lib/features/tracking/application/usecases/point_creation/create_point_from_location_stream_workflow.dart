import 'dart:async';
import 'package:dawarich/core/domain/models/point/local/local_point.dart';
import 'package:dawarich/features/tracking/application/repositories/location_provider_interface.dart';
import 'package:dawarich/features/tracking/application/usecases/point_creation/create_point_usecase.dart';
import 'package:dawarich/features/tracking/application/usecases/settings/get_tracker_settings_usecase.dart';
import 'package:dawarich/features/tracking/domain/enum/location_precision.dart';
import 'package:dawarich/features/tracking/domain/models/location_fix.dart';
import 'package:dawarich/features/tracking/domain/models/location_request.dart';
import 'package:dawarich/features/tracking/domain/models/tracker_settings.dart';
import 'package:flutter/foundation.dart';
import 'package:geolocator/geolocator.dart';
import 'package:option_result/result.dart';

/// Workflow for creating points using either:
/// - Auto mode (0): Event-driven tracking when location changes meaningfully
/// - Timer mode (>0): Fixed interval tracking
final class CreatePointFromLocationStreamWorkflow {
  final GetTrackerSettingsUseCase _getTrackerSettings;
  final ILocationProvider _locationProvider;
  final CreatePointUseCase _createPointFromLocationFix;

  CreatePointFromLocationStreamWorkflow(
    this._getTrackerSettings,
    this._locationProvider,
    this._createPointFromLocationFix,
  );

  /// Returns a stream of location points based on user settings.
  Stream<Result<LocalPoint, String>> getPointStream(int userId) async* {
    if (kDebugMode) {
      debugPrint('[LocationStream] Starting location stream for user $userId');
    }

    final TrackerSettings settings = await _getTrackerSettings(userId);
    final LocationPrecision precision = settings.locationPrecision;
    final int trackingFrequencySeconds = settings.trackingFrequency;
    final int minimumDistance = settings.minimumPointDistance;
    final bool isAutoMode = trackingFrequencySeconds == 0;

    if (kDebugMode) {
      debugPrint('[LocationStream] Settings: precision=$precision, frequency=${trackingFrequencySeconds}s, minDistance=${minimumDistance}m, autoMode=$isAutoMode');
    }

    if (isAutoMode) {
      yield* _getAutoModePointStream(
        userId,
        precision,
        minimumDistance,
        settings.statusUpdateInterval,
      );
    } else {
      yield* _getTimerPointStream(userId, precision, trackingFrequencySeconds);
    }

    if (kDebugMode) {
      debugPrint('[LocationStream] Location stream ended');
    }
  }

  /// Auto mode: track when the device has moved a meaningful distance.
  /// Also sends periodic heartbeat points when stationary (if [statusUpdateInterval] > 0).
  Stream<Result<LocalPoint, String>> _getAutoModePointStream(
    int userId,
    LocationPrecision precision,
    int minimumDistance,
    int statusUpdateInterval,
  ) async* {
    // Hybrid approach:
    // - If user set a minimum distance, use that (they know what's meaningful to them)
    // - Otherwise, derive from precision (reflects user's tracking mindset)
    final int distanceFilter = minimumDistance > 0
        ? minimumDistance
        : switch (precision) {
            LocationPrecision.best => 5,
            LocationPrecision.high => 5,
            LocationPrecision.balanced => 10,
            LocationPrecision.lowPower => 25,
          };

    final request = LocationRequest(
      precision: precision,
      distanceFilterMeters: distanceFilter,
      timeLimit: null,
      intervalDuration: const Duration(seconds: 5),
    );

    if (kDebugMode) {
      debugPrint('[LocationStream] Auto mode: distance filter = ${distanceFilter}m');
    }

    LocationFix? lastRecordedFix;
    bool isFirstPoint = true;
    DateTime? lastPointTime;
    Timer? heartbeatTimer;
    StreamSubscription<LocationFix>? _locationSub;
    final StreamController<Result<LocalPoint, String>> _pointController =
        StreamController<Result<LocalPoint, String>>.broadcast();

    /// Send a heartbeat point using the freshest available location fix.
    /// Yields the point directly to the point controller.
    Future<void> _sendHeartbeat(
      int userId,
      LocationPrecision precision,
      LocationRequest request,
    ) async {
      // Check if we already sent a point recently to avoid duplicates.
      // Cooldown is half the heartbeat interval to allow at most one missed
      // heartbeat when the location stream fires just before a heartbeat.
      final cooldownSeconds = statusUpdateInterval > 0 ? (statusUpdateInterval / 2).ceil() : 15;
      if (lastPointTime != null &&
          DateTime.now().difference(lastPointTime!).inSeconds < cooldownSeconds) {
        return;
      }

      try {
        // Try to get a fresh location fix
        final result = await _locationProvider.getCurrent(request);

        if (result case Ok(value: final fix)) {
          final timestamp = DateTime.now().toUtc();
          final pointResult =
              await _createPointFromLocationFix(fix, timestamp, userId, isHeartbeat: true);

          if (pointResult case Ok(value: final point)) {
            lastPointTime = DateTime.now();
            lastRecordedFix = fix;
            if (kDebugMode) {
              debugPrint('[LocationStream] Heartbeat point sent');
            }
            _pointController.add(Ok(point));
          }
        }
      } catch (e, s) {
        if (kDebugMode) {
          debugPrint('[LocationStream] Heartbeat error: $e\n$s');
        }
      }
    }

    /// Start (or restart) the heartbeat timer.
    /// Fires at [statusUpdateInterval] intervals to send a point even when stationary.
    void startHeartbeat() {
      heartbeatTimer?.cancel();
      if (statusUpdateInterval <= 0) return;

      heartbeatTimer = Timer.periodic(
        Duration(seconds: statusUpdateInterval),
        (_) async {
          await _sendHeartbeat(userId, precision, request);
        },
      );

      if (kDebugMode) {
        debugPrint('[LocationStream] Heartbeat timer started (interval: ${statusUpdateInterval}s)');
      }
    }

    /// Cancel the heartbeat timer and close the controller.
    void stopHeartbeat() {
      heartbeatTimer?.cancel();
      heartbeatTimer = null;
      _locationSub?.cancel();
      _locationSub = null;
      _pointController.close();
    }

    /// Process a location fix from the stream.
    void _processLocationFix(
      LocationFix fix,
      int userId,
      LocationPrecision precision,
      LocationRequest request,
      int distanceFilter,
    ) {
      if (isFirstPoint) {
        isFirstPoint = false;
        lastRecordedFix = fix;
        lastPointTime = DateTime.now();

        if (kDebugMode) {
          debugPrint('[LocationStream] Auto: Recording initial location');
        }

        final timestamp = DateTime.now().toUtc();
        final pointResult = _createPointFromLocationFix(fix, timestamp, userId);

        pointResult.then((result) {
          if (result case Ok(value: final point)) {
            _pointController.add(Ok(point));
          }
        });

        // Start heartbeat timer after first point
        startHeartbeat();
        return;
      }

      // Subsequent points are filtered
      if (_shouldRecordPoint(lastRecordedFix, fix, distanceFilter)) {
        if (kDebugMode) {
          debugPrint('[LocationStream] Auto: Recording new location');
        }

        final timestamp = DateTime.now().toUtc();
        final pointResult = _createPointFromLocationFix(fix, timestamp, userId);

        pointResult.then((result) {
          if (result case Ok(value: final point)) {
            lastRecordedFix = fix;
            lastPointTime = DateTime.now();
            // Reset heartbeat timer on new point
            startHeartbeat();
            _pointController.add(Ok(point));
          } else if (result case Err(value: final err)) {
            if (kDebugMode) {
              debugPrint('[LocationStream] Point creation failed: $err');
            }
          }
        });
      } else if (kDebugMode) {
        debugPrint('[LocationStream] Auto: Skipping similar location');
      }
    }

    try {
      // Listen to location stream, first emission becomes the initial point
      final locationStream = _locationProvider.getLocationStream(request);

      // Forward location fixes to the controller, store subscription for cleanup
      _locationSub = locationStream.listen(
        (fix) {
          // Process location fix immediately
          _processLocationFix(fix, userId, precision, request, distanceFilter);
        },
        onError: (e) {
          if (kDebugMode) {
            debugPrint('[LocationStream] Location stream error: $e');
          }
        },
        onDone: () {
          if (kDebugMode) {
            debugPrint('[LocationStream] Location stream ended naturally');
          }
          _pointController.close();
        },
      );

      // Start heartbeat timer after first point
      // (will be started by _processLocationFix on first fix)

      // Yield all points (location + heartbeat) from the single controller
      await for (final point in _pointController.stream) {
        yield point;
      }
    } catch (e, s) {
      stopHeartbeat();
      if (kDebugMode) {
        debugPrint('[LocationStream] Auto mode error: $e\n$s');
      }
      yield Err('Location stream error: $e');
    } finally {
      stopHeartbeat();
    }
  }

  /// Check if we should record this point (new location or periodic stationary update).
  bool _shouldRecordPoint(LocationFix? last, LocationFix current, int minDistMeters) {
    if (last == null) {
      return true;
    }

    final dist = Geolocator.distanceBetween(last.latitude, last.longitude, current.latitude, current.longitude);
    if (dist >= minDistMeters) {
      return true;
    }

    final timeDiff = current.timestampUtc.difference(last.timestampUtc).inSeconds;
    return timeDiff > 60;
  }

  /// Timer mode: emit points at fixed intervals using cached location.
  Stream<Result<LocalPoint, String>> _getTimerPointStream(
    int userId,
    LocationPrecision precision,
    int frequencySeconds,
  ) async* {
    LocationFix? latestFix;
    StreamSubscription<LocationFix>? locationSub;

    final int minSeconds = 1;
    final int intervalSeconds =
    (frequencySeconds / 2).ceil().clamp(minSeconds, frequencySeconds);

    final intervalDuration = Duration(seconds: intervalSeconds);

    final request = LocationRequest(
      precision: precision,
      distanceFilterMeters: 0,
      timeLimit: null,
      // Poll at half the tracking frequency so we usually have a fresh fix
      intervalDuration: intervalDuration,
    );

    try {
      final locationStream = _locationProvider.getLocationStream(request);
      locationSub = locationStream.listen(
        (fix) {
          latestFix = fix;
          if (kDebugMode) {
            debugPrint('[LocationStream] Cache updated: ${fix.latitude}, ${fix.longitude}');
          }
        },
        onError: (e) {
          if (kDebugMode) {
            debugPrint('[LocationStream] Stream error: $e');
          }
        },
      );

      final controller = StreamController<Result<LocalPoint, String>>();

      final initialResult = await _locationProvider.getCurrent(request);
      if (initialResult case Ok(value: final fix)) {
        latestFix = fix;
        final timestamp = DateTime.now().toUtc();
        final pointResult = await _createPointFromLocationFix(fix, timestamp, userId);
        if (pointResult case Ok(value: final point)) {
          yield Ok(point);
        }
      }

      final timerDuration = Duration(seconds: frequencySeconds);

      if (kDebugMode) {
        debugPrint('[LocationStream] Timer mode: interval = ${frequencySeconds}s');
      }

      Timer.periodic(timerDuration, (timer) async {
        if (controller.isClosed) {
          timer.cancel();
          return;
        }

        LocationFix? fixToUse = latestFix;

        if (fixToUse == null ||
            DateTime.now().difference(fixToUse.timestampUtc).inSeconds > 30) {
          if (kDebugMode) {
            debugPrint('[LocationStream] Cache stale, fetching current position');
          }
          final currentResult = await _locationProvider.getCurrent(request);
          if (currentResult case Ok(value: final fix)) {
            fixToUse = fix;
            latestFix = fix;
          }
        }

        if (fixToUse == null) {
          controller.add(Err('No location available'));
          return;
        }

        try {
          final timestamp = DateTime.now().toUtc();
          final pointResult = await _createPointFromLocationFix(fixToUse, timestamp, userId);

          if (pointResult case Ok(value: final point)) {
            controller.add(Ok(point));
          } else if (pointResult case Err(value: final err)) {
            if (kDebugMode) {
              debugPrint('[LocationStream] Point validation failed: $err');
            }
            controller.add(Err('Failed to create point: $err'));
          }
        } catch (e) {
          if (kDebugMode) {
            debugPrint('[LocationStream] Error creating point: $e');
          }
          controller.add(Err('Failed to create point: $e'));
        }
      });

      await for (final result in controller.stream) {
        yield result;
      }

      await locationSub.cancel();
    } catch (e, s) {
      if (kDebugMode) {
        debugPrint('[LocationStream] Timer error: $e\n$s');
      }
      yield Err('Location stream error: $e');
      await locationSub?.cancel();
    }
  }
}
