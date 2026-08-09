import 'package:dawarich/core/data/drift/database/sqlite_client.steps.dart';
import 'package:drift/drift.dart';

Future<void> migrateToV10(Migrator m, Schema10 schema) async {
  // Column already added in v10 schema definition.
  // No data migration needed — statusUpdateInterval defaults to 0 (disabled).
}
