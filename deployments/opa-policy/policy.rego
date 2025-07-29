package flipt.authz.v1
import rego.v1

default allow := false

# Allow access to default namespace
allow if {
  claims := json.unmarshal(input.authentication.metadata["io.flipt.auth.claims"])
  namespace := extract_namespace(input.request)
  namespace == "default"
}

# Allow access based on allowed_namespaces claim
allow if {
  claims := json.unmarshal(input.authentication.metadata["io.flipt.auth.claims"])
  allowed_raw := claims.allowed_namespaces
  allowed := json.unmarshal(allowed_raw)
  namespace := extract_namespace(input.request)
  namespace in allowed
}

extract_namespace(req) = ns if {
  ns := req.namespace
} else = ns if {
  ns := req.path_params.namespaceKey
}