package pgbench

import (
	"fmt"
)

type ScriptType string

const (
	ScriptInsert ScriptType = "insert"
	ScriptSelect ScriptType = "select"
	ScriptUpdate ScriptType = "update"
)

func GenerateInsertScript(keyType, tableName string) string {
	switch keyType {
	case "sequential":
		return fmt.Sprintf(`INSERT INTO %s (data) VALUES ('test_data_' || :client_id);`, tableName)

	case "uuidv4":
		return fmt.Sprintf(`INSERT INTO %s (id, data) VALUES (gen_random_uuid(), 'test_data_' || :client_id);`, tableName)

	case "uuidv7":
		return fmt.Sprintf(`INSERT INTO %s (id, data) VALUES (uuidv7(), 'test_data_' || :client_id);`, tableName)

	case "uuidv1":
		return fmt.Sprintf(`INSERT INTO %s (id, data) VALUES (uuid_generate_v1(), 'test_data_' || :client_id);`, tableName)

	case "ulid":
		return fmt.Sprintf(`INSERT INTO %s (id, data) VALUES (gen_ulid(), 'test_data_' || :client_id);`, tableName)

	case "ulid_monotonic":
		return fmt.Sprintf(`INSERT INTO %s (id, data) VALUES (gen_monotonic_ulid(), 'test_data_' || :client_id);`, tableName)

	default:
		return fmt.Sprintf(`-- Unknown key type: %s`, keyType)
	}
}

func GenerateSelectScript(keyType, tableName string) string {
	lookupTable := tableName + "_ids"
	return fmt.Sprintf(`\set rn random(1, :num_records)
SELECT * FROM %s WHERE id = (SELECT id FROM %s WHERE rn = :rn);`, tableName, lookupTable)
}

func GenerateUpdateScript(keyType, tableName string) string {
	lookupTable := tableName + "_ids"
	return fmt.Sprintf(`\set rn random(1, :num_records)
UPDATE %s SET data = 'updated_' || :client_id WHERE id = (SELECT id FROM %s WHERE rn = :rn);`, tableName, lookupTable)
}

// pgbench doesn't support weighted random selection, so we use conditional logic
func GenerateMixedScript(keyType, tableName string, insertWeight, readWeight, updateWeight int) string {
	if insertWeight+readWeight+updateWeight != 100 {
		return fmt.Sprintf(`-- Error: Weights must sum to 100 (got %d)`, insertWeight+readWeight+updateWeight)
	}

	insertScript := GenerateInsertScript(keyType, tableName)
	selectScript := GenerateSelectScript(keyType, tableName)
	updateScript := GenerateUpdateScript(keyType, tableName)

	readThreshold := insertWeight
	updateThreshold := insertWeight + readWeight

	return fmt.Sprintf(`\set rand random(1, 100)
\set insert_threshold %d
\set update_threshold %d

-- Execute based on random value
-- 1-%d: INSERT
-- %d-%d: SELECT
-- %d-100: UPDATE

\if :rand <= :insert_threshold
  %s
\elif :rand <= :update_threshold
  %s
\else
  %s
\endif`, readThreshold, updateThreshold, readThreshold, readThreshold+1, updateThreshold, updateThreshold+1, insertScript, selectScript, updateScript)
}

// pgbench executes one SQL statement per transaction by default
func GenerateMultipleInserts(keyType, tableName string, batchSize int) string {
	if batchSize <= 1 {
		return GenerateInsertScript(keyType, tableName)
	}

	script := "BEGIN;\n"
	for i := 0; i < batchSize; i++ {
		script += GenerateInsertScript(keyType, tableName) + "\n"
	}
	script += "COMMIT;"

	return script
}
