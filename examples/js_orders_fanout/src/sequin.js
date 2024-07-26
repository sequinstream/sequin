import { Client } from "@sequinstream/sequin-js";
import dotenv from "dotenv";

dotenv.config();

const baseUrl = process.env.SEQUIN_URL || "http://localhost:7376";
const sequin = Client.init({ baseUrl });

export { sequin, baseUrl };
