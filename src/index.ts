/**
 * @slothwrk/polishd — isomorphic entry point.
 *
 * Safe to import anywhere (client or server). Exposes only config + the event
 * schema; nothing here touches Node, the DOM, or the database. For capture use
 * `@slothwrk/polishd/client`, for storage/queries `@slothwrk/polishd/server`.
 */
export * from "./config";
export * from "./shared/types";
