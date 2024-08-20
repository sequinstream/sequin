import * as Components from "../svelte/**/*.svelte"
import {getRender} from "live_svelte"

export const render = getRender(Components)
