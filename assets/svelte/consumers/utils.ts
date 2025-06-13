export function externalType(type: string) {
  switch (type) {
    case "http_push":
      return "webhook";
    default:
      return type.replace(/_/g, "-");
  }
}
