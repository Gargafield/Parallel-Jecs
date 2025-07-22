--!optimize 2
--!native

local jecs = require("@Packages/Jecs")

local serdes = {}

local sizes = {
    [1] = 8, -- number
    [2] = 24, -- vector3
}

local function getType(value)
    if type(value) == "number" then
        return 1
    elseif type(value) == "vector" then
        return 2
    else
        error("Unsupported type: " .. tostring(type(value)))
    end
end


function serdes.serializeColumnsFast(columns: { { any } }, count: number, distribution: number)
    -- first get types of columns
    local types = {}
    local rowSize = 0
    for i = 1, #columns do
        local _type = getType(columns[i][1])
        types[i] = _type
        rowSize += sizes[_type]
    end

    -- create buffers
    local numPartitions = math.ceil(count / distribution)
    local buffers = {}
    local partitions = { }
    for i = 1, numPartitions do
        buffers[i] = buffer.create(rowSize * distribution)
        partitions[i] = { 0, 0, 0 } -- start index, count, offset
    end

    for i = 1, #columns do
        local column = columns[i]
        if types[i] == 1 then -- number
            local counter = count
            for partitionIndex = 1, numPartitions do
                local buf = buffers[partitionIndex]
                local start = count - counter
                local localCount = math.min(distribution, counter)
                counter -= localCount
                local partition = partitions[partitionIndex]
                partition[1] = start
                partition[2] = localCount
                local offset = partition[3]
                for j = 0, localCount - 1 do
                    buffer.writef64(buf, offset + j * 8, column[start + j + 1])
                end
                partition[3] += localCount * 8
            end
        elseif types[i] == 2 then -- vector
            local counter = count
            for partitionIndex = 1, numPartitions do
                local buf = buffers[partitionIndex]
                local start = count - counter
                local localCount = math.min(distribution, counter)
                counter -= localCount
                local partition = partitions[partitionIndex]
                partition[1] = start
                partition[2] = localCount
                local offset = partition[3]
                for j = 0, localCount - 1 do
                    local vec = column[start + j + 1]
                    local _index = offset + j * 24
                    buffer.writef64(buf, _index, vec.x)
                    buffer.writef64(buf, _index + 8, vec.y)
                    buffer.writef64(buf, _index + 16, vec.z)
                end
                partition[3] += localCount * 24
            end
        else
            error("Unsupported type: " .. tostring(types[i]))
        end
    end
    return buffers, partitions, types
end


function serdes.serializeColumns(columns: { { any } }, offset: number, count: number)
    -- first get types of columns
    local types = {}
    local rowSize = 0
    for i = 1, #columns do
        local _type = getType(columns[i][1])
        types[i] = _type
        rowSize += sizes[_type]
    end

    -- create buffer
    local buf = buffer.create(rowSize * count)
    local index = 0
    for i = 1, #columns do
        local column = columns[i]
        if types[i] == 1 then -- number
            for j = 0, count - 1 do
                buffer.writef64(buf, index + j * 8, column[offset + j])
            end
            index += count * 8
        elseif types[i] == 2 then -- vector
            for j = 0, count - 1 do
                local vec = column[offset + j]
                local _index = index + j * 24
                buffer.writef64(buf, _index, vec.x)
                buffer.writef64(buf, _index + 8, vec.y)
                buffer.writef64(buf, _index + 16, vec.z)
            end
            index += count * 24
        else
            error("Unsupported type: " .. tostring(types[i]))
        end
    end

    return buf, types
end

function serdes.serializeColumnsInto(buf: buffer, columns: { { any } }, offset: number, count: number)
    -- first get types of columns
    local types = {}
    local rowSize = 0
    for i = 1, #columns do
        local _type = getType(columns[i][1])
        types[i] = _type
        rowSize += sizes[_type]
    end

    local index = 0
    for i = 1, #columns do
        local column = columns[i]
        if types[i] == 1 then -- number
            for j = 0, count - 1 do
                buffer.writef64(buf, index + j * 8, column[offset + j])
            end
            index += count * 8
        elseif types[i] == 2 then -- vector
            for j = 0, count - 1 do
                local vec = column[offset + j]
                local _index = index + j * 24
                buffer.writef64(buf, _index, vec.x)
                buffer.writef64(buf, _index + 8, vec.y)
                buffer.writef64(buf, _index + 16, vec.z)
            end
            index += count * 24
        else
            error("Unsupported type: " .. tostring(types[i]))
        end
    end

    return buf, types
end


function serdes.deserializeColumns(buf: buffer, types: { number }, count: number)
    local columns: { { any } } = {}
    local index = 0
    for i = 1, #types do
        local column = {}
        columns[i] = column
        if types[i] == 1 then -- number
            for j = 0, count - 1 do
                column[j + 1] = buffer.readf64(buf, index + j * 8)
            end
            index += count * 8
        elseif types[i] == 2 then -- vector
            for j = 0, count - 1 do
                local _index = index + j * 24
                local a = buffer.readf64(buf, _index)
                local b = buffer.readf64(buf, _index + 8)
                local c = buffer.readf64(buf, _index + 16)
                column[j + 1] = vector.create(a, b, c)
            end
            index += count * 24
        end
    end
    return columns
end

function serdes.deserializeColumnsInto(columns: { {any} }, buf: buffer, types: { number }, count: number, offset: number?)
    local offset = offset or 0
    local index = 0
    for i = 1, #types do
        local column
        if columns[i] then
            column = columns[i]
        else
            column = table.create(count)
            columns[i] = column
        end
        if types[i] == 1 then -- number
            for j = 0, count - 1 do
                column[offset + j + 1] = buffer.readf64(buf, index + j * 8)
            end
            index += count * 8
        elseif types[i] == 2 then -- vector
            for j = 0, count - 1 do
                local _index = index + j * 24
                local a = buffer.readf64(buf, _index)
                local b = buffer.readf64(buf, _index + 8)
                local c = buffer.readf64(buf, _index + 16)
                column[offset + j + 1] = vector.create(a, b, c)
            end
            index += count * 24
        end
    end
    return columns
end

return serdes
