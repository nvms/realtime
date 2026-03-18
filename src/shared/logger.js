import log from "@prsm/log"

export const LogLevel = {
  NONE: 0,
  ERROR: 1,
  WARN: 2,
  INFO: 3,
  DEBUG: 4,
}

const levelMap = { 0: "none", 1: "error", 2: "warn", 3: "info", 4: "debug" }

export function configureLogLevel(level) {
  const str = typeof level === "number" ? levelMap[level] ?? "info" : level
  log.configure({ level: str })
}

export const serverLogger = log.child({ sys: "server" })
export const clientLogger = log.child({ sys: "client" })
export const logger = log
