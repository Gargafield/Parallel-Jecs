
local process = require("@lune/process")
local task = require("@lune/task")

local processssss : {[string]: process.ChildProcess} = {}

local function maybeWatch(program: string, params: { string }) : process.ChildProcess
    local result = process.exec(program, params)
    if not result.code then
        print(`Failed to start {program}`)
        print(`Status code: {result.code}`)
        print(result.stderr)
        process.exit(1)
    end
    table.insert(params, "--watch")
    return process.create(program, params)
end

processssss["Rojo Sourcemap"] = maybeWatch("rojo", {"sourcemap", "-o", "sourcemap.json", "sourcemap.project.json"})

process.exec("wally-package-types", {"--sourcemap", "sourcemap.json", "Packages/"})

processssss["Darklua Src"] = maybeWatch("darklua", {"process", "src", "out"})
-- processssss["Darklua Packages"] = maybeWatch("darklua", {"process", "Packages", "out/Packages"})
processssss["Rojo Serve"] = process.create("rojo", {"serve", "default.project.json"})

for name, p in processssss do
    local readTask = task.spawn(function()
        while true do
            local read = p.stdout:read()
            if read then
                print(`{name} > {read}`)
            end
            task.wait(1)
        end
    end)

    task.spawn(function()
        local status = p:status()
        task.cancel(readTask)

        print(`{name} exited with code {status.code}`)
        if status.code ~= 0 or not status.ok then
            print(p.stderr:read())
        end

        processssss[name] = nil
    end)
end

while (next(processssss)) do
    task.wait(1)
end

print("Bye bye!")
