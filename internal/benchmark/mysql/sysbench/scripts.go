package sysbench

import (
	"fmt"
)

type ScriptType string

const (
	ScriptInsert ScriptType = "insert"
	ScriptSelect ScriptType = "select"
	ScriptUpdate ScriptType = "update"
)

// GenerateInsertScript generates a sysbench Lua script for INSERT operations
// UUIDs are generated client-side in Go and passed to sysbench
func GenerateInsertScript(keyType, tableName string) string {
	switch keyType {
	case "sequential":
		return fmt.Sprintf(`
-- Sysbench INSERT script for BIGINT AUTO_INCREMENT
sysbench.cmdline.options = {
	table_name = {"Table name", "%s"},
}

function thread_init()
	drv = sysbench.sql.driver()
	con = drv:connect()
end

function thread_done()
	con:disconnect()
end

function event()
	con:query("INSERT INTO " .. sysbench.opt.table_name .. " (data) VALUES ('test_data_" .. sysbench.tid .. "')")
end
`, tableName)

	case "uuidv4", "uuidv7", "uuidv1", "ulid", "ulid_monotonic":
		// For UUID types, we generate random binary values in Lua
		// sysbench.rand.unique() provides unique values per thread
		return fmt.Sprintf(`
-- Sysbench INSERT script for UUID/ULID types (BINARY(16))
sysbench.cmdline.options = {
	table_name = {"Table name", "%s"},
}

function thread_init()
	drv = sysbench.sql.driver()
	con = drv:connect()
end

function thread_done()
	con:disconnect()
end

function event()
	-- Generate a random 16-byte value as hex (32 chars)
	-- Using two random values to fill 128 bits
	local hi = sysbench.rand.unique()
	local lo = sysbench.rand.unique()
	local uuid_hex = string.format("%%016x%%016x", hi, lo)
	con:query("INSERT INTO " .. sysbench.opt.table_name .. " (id, data) VALUES (UNHEX('" .. uuid_hex .. "'), 'test_data_" .. sysbench.tid .. "')")
end
`, tableName)

	default:
		return fmt.Sprintf(`-- Unknown key type: %s`, keyType)
	}
}

// GenerateSelectScript generates a sysbench Lua script for SELECT operations
func GenerateSelectScript(keyType, tableName string) string {
	switch keyType {
	case "sequential":
		return fmt.Sprintf(`
-- Sysbench SELECT script for BIGINT PRIMARY KEY
sysbench.cmdline.options = {
	table_name = {"Table name", "%s"},
	num_records = {"Number of records", 0},
}

function thread_init()
	drv = sysbench.sql.driver()
	con = drv:connect()
end

function thread_done()
	con:disconnect()
end

function event()
	local id = sysbench.rand.uniform(1, sysbench.opt.num_records)
	con:query("SELECT * FROM " .. sysbench.opt.table_name .. " WHERE id = " .. id)
end
`, tableName)

	case "uuidv4", "uuidv7", "uuidv1", "ulid", "ulid_monotonic":
		// For UUID types, we need to select by offset since we don't know the IDs
		return fmt.Sprintf(`
-- Sysbench SELECT script for UUID/ULID types
sysbench.cmdline.options = {
	table_name = {"Table name", "%s"},
	num_records = {"Number of records", 0},
}

function thread_init()
	drv = sysbench.sql.driver()
	con = drv:connect()
end

function thread_done()
	con:disconnect()
end

function event()
	local offset = sysbench.rand.uniform(0, sysbench.opt.num_records - 1)
	con:query("SELECT * FROM " .. sysbench.opt.table_name .. " LIMIT 1 OFFSET " .. offset)
end
`, tableName)

	default:
		return fmt.Sprintf(`-- Unknown key type: %s`, keyType)
	}
}

// GenerateUpdateScript generates a sysbench Lua script for UPDATE operations
func GenerateUpdateScript(keyType, tableName string) string {
	switch keyType {
	case "sequential":
		return fmt.Sprintf(`
-- Sysbench UPDATE script for BIGINT PRIMARY KEY
sysbench.cmdline.options = {
	table_name = {"Table name", "%s"},
	num_records = {"Number of records", 0},
}

function thread_init()
	drv = sysbench.sql.driver()
	con = drv:connect()
end

function thread_done()
	con:disconnect()
end

function event()
	local id = sysbench.rand.uniform(1, sysbench.opt.num_records)
	con:query("UPDATE " .. sysbench.opt.table_name .. " SET data = 'updated_" .. sysbench.tid .. "' WHERE id = " .. id)
end
`, tableName)

	case "uuidv4", "uuidv7", "uuidv1", "ulid", "ulid_monotonic":
		return fmt.Sprintf(`
-- Sysbench UPDATE script for UUID/ULID types
sysbench.cmdline.options = {
	table_name = {"Table name", "%s"},
	num_records = {"Number of records", 0},
}

function thread_init()
	drv = sysbench.sql.driver()
	con = drv:connect()
end

function thread_done()
	con:disconnect()
end

function event()
	local offset = sysbench.rand.uniform(0, sysbench.opt.num_records - 1)
	con:query("UPDATE " .. sysbench.opt.table_name .. " SET data = 'updated_" .. sysbench.tid .. "' WHERE id = (SELECT id FROM (SELECT id FROM " .. sysbench.opt.table_name .. " LIMIT 1 OFFSET " .. offset .. ") AS tmp)")
end
`, tableName)

	default:
		return fmt.Sprintf(`-- Unknown key type: %s`, keyType)
	}
}

// GenerateMixedScript generates a sysbench Lua script for mixed workloads
func GenerateMixedScript(keyType, tableName string, insertWeight, readWeight, updateWeight int) string {
	if insertWeight+readWeight+updateWeight != 100 {
		return fmt.Sprintf(`-- Error: Weights must sum to 100 (got %d)`, insertWeight+readWeight+updateWeight)
	}

	insertThreshold := insertWeight
	readThreshold := insertWeight + readWeight

	switch keyType {
	case "sequential":
		return fmt.Sprintf(`
-- Sysbench MIXED script for BIGINT AUTO_INCREMENT
-- Weights: INSERT=%d%%, READ=%d%%, UPDATE=%d%%
sysbench.cmdline.options = {
	table_name = {"Table name", "%s"},
	num_records = {"Number of records", 0},
}

local insert_threshold = %d
local read_threshold = %d

function thread_init()
	drv = sysbench.sql.driver()
	con = drv:connect()
end

function thread_done()
	con:disconnect()
end

function event()
	local op = sysbench.rand.uniform(1, 100)

	if op <= insert_threshold then
		-- INSERT
		con:query("INSERT INTO " .. sysbench.opt.table_name .. " (data) VALUES ('test_data_" .. sysbench.tid .. "')")
	elseif op <= read_threshold then
		-- SELECT
		local id = sysbench.rand.uniform(1, math.max(1, sysbench.opt.num_records))
		con:query("SELECT * FROM " .. sysbench.opt.table_name .. " WHERE id = " .. id)
	else
		-- UPDATE
		local id = sysbench.rand.uniform(1, math.max(1, sysbench.opt.num_records))
		con:query("UPDATE " .. sysbench.opt.table_name .. " SET data = 'updated_" .. sysbench.tid .. "' WHERE id = " .. id)
	end
end
`, insertWeight, readWeight, updateWeight, tableName, insertThreshold, readThreshold)

	case "uuidv4", "uuidv7", "uuidv1", "ulid", "ulid_monotonic":
		return fmt.Sprintf(`
-- Sysbench MIXED script for UUID/ULID types
-- Weights: INSERT=%d%%, READ=%d%%, UPDATE=%d%%
sysbench.cmdline.options = {
	table_name = {"Table name", "%s"},
	num_records = {"Number of records", 0},
}

local insert_threshold = %d
local read_threshold = %d

function thread_init()
	drv = sysbench.sql.driver()
	con = drv:connect()
end

function thread_done()
	con:disconnect()
end

function event()
	local op = sysbench.rand.uniform(1, 100)

	if op <= insert_threshold then
		-- INSERT with random UUID-like value
		local hi = sysbench.rand.unique()
		local lo = sysbench.rand.unique()
		local uuid_hex = string.format("%%016x%%016x", hi, lo)
		con:query("INSERT INTO " .. sysbench.opt.table_name .. " (id, data) VALUES (UNHEX('" .. uuid_hex .. "'), 'test_data_" .. sysbench.tid .. "')")
	elseif op <= read_threshold then
		-- SELECT by offset
		local offset = sysbench.rand.uniform(0, math.max(0, sysbench.opt.num_records - 1))
		con:query("SELECT * FROM " .. sysbench.opt.table_name .. " LIMIT 1 OFFSET " .. offset)
	else
		-- UPDATE by offset
		local offset = sysbench.rand.uniform(0, math.max(0, sysbench.opt.num_records - 1))
		con:query("UPDATE " .. sysbench.opt.table_name .. " SET data = 'updated_" .. sysbench.tid .. "' WHERE id = (SELECT id FROM (SELECT id FROM " .. sysbench.opt.table_name .. " LIMIT 1 OFFSET " .. offset .. ") AS tmp)")
	end
end
`, insertWeight, readWeight, updateWeight, tableName, insertThreshold, readThreshold)

	default:
		return fmt.Sprintf(`-- Unknown key type: %s`, keyType)
	}
}

// GenerateBatchInsertScript generates a script for batch inserts
func GenerateBatchInsertScript(keyType, tableName string, batchSize int) string {
	if batchSize <= 1 {
		return GenerateInsertScript(keyType, tableName)
	}

	switch keyType {
	case "sequential":
		// Build VALUES clause for batch insert
		return fmt.Sprintf(`
-- Sysbench BATCH INSERT script for BIGINT AUTO_INCREMENT (batch size: %d)
sysbench.cmdline.options = {
	table_name = {"Table name", "%s"},
}

function thread_init()
	drv = sysbench.sql.driver()
	con = drv:connect()
end

function thread_done()
	con:disconnect()
end

function event()
	local values = {}
	for i = 1, %d do
		table.insert(values, "('test_data_" .. sysbench.tid .. "_" .. i .. "')")
	end
	con:query("INSERT INTO " .. sysbench.opt.table_name .. " (data) VALUES " .. table.concat(values, ","))
end
`, batchSize, tableName, batchSize)

	case "uuidv4", "uuidv7", "uuidv1", "ulid", "ulid_monotonic":
		return fmt.Sprintf(`
-- Sysbench BATCH INSERT script for UUID/ULID types (batch size: %d)
sysbench.cmdline.options = {
	table_name = {"Table name", "%s"},
}

function thread_init()
	drv = sysbench.sql.driver()
	con = drv:connect()
end

function thread_done()
	con:disconnect()
end

function event()
	local values = {}
	for i = 1, %d do
		local hi = sysbench.rand.unique()
		local lo = sysbench.rand.unique()
		local uuid_hex = string.format("%%016x%%016x", hi, lo)
		table.insert(values, "(UNHEX('" .. uuid_hex .. "'), 'test_data_" .. sysbench.tid .. "_" .. i .. "')")
	end
	con:query("INSERT INTO " .. sysbench.opt.table_name .. " (id, data) VALUES " .. table.concat(values, ","))
end
`, batchSize, tableName, batchSize)

	default:
		return fmt.Sprintf(`-- Unknown key type: %s`, keyType)
	}
}
