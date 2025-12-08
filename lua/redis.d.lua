---@diagnostic disable: missing-fields
---@meta

---@class Redis
---@field call fun(command: string, ...: any): any
---@field pcall fun(command: string, ...: any): any, any
---@field error_reply fun(message: string): table
---@field status_reply fun(message: string): table
---@field log fun(level: integer, message: string): nil
---@field sha1hex fun(script: string): string
---@field breakpoint fun(): nil
---@field debug fun(action: string): any

---@type Redis
redis = {}

---@type table<string>
KEYS = {}

---@type table<string>
ARGV = {}

