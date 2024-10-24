import { getRender } from "live_svelte";
import * as Components from "../svelte/**/*.svelte";

export const render = getRender(Components);
