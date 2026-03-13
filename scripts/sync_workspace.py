import os
import sys
import json
import urllib3
from typing import Any, Iterable
from msal import PublicClientApplication

# Desabilita aviso de certificado, já que cert_reqs='CERT_NONE' está sendo usado
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FABRIC_BASE_URL = "https://api.fabric.microsoft.com/v1"
PBI_SCOPE = ["https://analysis.windows.net/powerbi/api/.default"]

http = urllib3.PoolManager(cert_reqs='CERT_NONE')


def pretty_print_json(title: str, data: Any) -> None:
    """
    Imprime um JSON de forma bonita/indentada no log.
    Aceita dict, list ou qualquer objeto serializável em JSON.
    """
    print(f"\n===== {title} =====")
    try:
        print(json.dumps(data, indent=2, ensure_ascii=False))
    except TypeError:
        # Fallback se for algo não serializável
        print(data)
    print("===== FIM =====\n")


def get_env_or_exit(var_name: str) -> str:
    """
    Lê variável de ambiente ou encerra o programa com erro.
    """
    value = os.environ.get(var_name)
    if not value:
        print(f"Erro: variável de ambiente '{var_name}' não definida.")
        sys.exit(1)
    return value


def get_access_token(authentication: dict) -> str:
    """
    Obtém o access token via ROPC (username/password) usando MSAL.
    """
    authority = f"https://login.microsoftonline.com/{authentication['tenant_id']}"

    app = PublicClientApplication(
        client_id=authentication["client_id"],
        authority=authority,
    )

    token_response = app.acquire_token_by_username_password(
        username=authentication["username"],
        password=authentication["password"],
        scopes=PBI_SCOPE,
    )

    access_token = token_response.get("access_token")
    if not access_token:
        error_description = token_response.get("error_description") or token_response
        print(f"Erro ao obter token: {error_description}")
        sys.exit(1)

    return access_token


def fabric_request(
    method: str,
    path: str,
    headers: dict,
    *,
    json_body: dict | None = None,
    expected_status: Iterable[int] = (200,),
    description: str = "",
) -> dict:
    """
    Envia uma requisição para a API do Fabric e retorna o JSON já decodificado.
    Faz checagem básica de status code.
    """
    url = f"{FABRIC_BASE_URL}{path}"

    body_bytes = None
    if json_body is not None:
        body_bytes = json.dumps(json_body).encode("utf-8")

    resp = http.request(
        method,
        url=url,
        headers=headers,
        body=body_bytes,
    )

    try:
        data = json.loads(resp.data.decode("utf-8")) if resp.data else {}
    except json.JSONDecodeError:
        data = {"raw": resp.data.decode("utf-8", errors="replace")}

    if resp.status not in expected_status:
        msg = (
            f"Erro ao chamar API do Fabric ({description or path}): "
            f"HTTP {resp.status} - {data}"
        )
        print(msg)
        sys.exit(1)

    return data


def sync_pbi_workspace(workspace_id: str, connection_id: str, authentication: dict) -> None:
    """
    Sincroniza o Power BI Workspace com o repositório Git remoto.
    """
    # === AUTENTICAÇÃO ===
    access_token = get_access_token(authentication)

    request_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # === CREDENCIAIS GIT ===
    git_credentials = fabric_request(
        "GET",
        f"/workspaces/{workspace_id}/git/myGitCredentials",
        headers=request_headers,
        description="myGitCredentials",
    )

    pretty_print_json("Resposta da API do Fabric - myGitCredentials", git_credentials)

    if git_credentials.get("source") == "None":
        conf_body = {
            "source": "ConfiguredConnection",
            "connectionId": connection_id,
        }

        conf_credentials = fabric_request(
            "PATCH",
            f"/workspaces/{workspace_id}/git/myGitCredentials",
            headers=request_headers,
            json_body=conf_body,
            expected_status=(200, 201, 202),
            description="configuração de credenciais Git",
        )

        pretty_print_json(
            "Resposta da API do Fabric - configuração de credenciais Git",
            conf_credentials,
        )
    
    # === STATUS DE SINCRONIZAÇÃO ===
    status = fabric_request(
        "GET",
        f"/workspaces/{workspace_id}/git/status",
        headers=request_headers,
        description="status de sincronização da workspace",
    )

    pretty_print_json(
        "Resposta da API do Fabric - status de sincronização da workspace", status
    )

    git_commit_workspace = status.get("workspaceHead")
    git_commit_repo = status.get("remoteCommitHash")

    print(f"Workspace Commit: {git_commit_workspace}")
    print(f"Repo Commit: {git_commit_repo}")

    if not git_commit_workspace or not git_commit_repo:
        print(
            "Não foi possível obter os commits da workspace ou do repositório. "
            "Verifique a configuração da workspace e da conexão Git."
        )
        sys.exit(1)

    # === ATUALIZAÇÃO DA WORKSPACE ===
    if git_commit_workspace != git_commit_repo:
        print("Há mudanças não publicadas na workspace! Iniciando sincronização...")

        update_body = {
            "workspaceHead": git_commit_workspace,
            "remoteCommitHash": git_commit_repo,
            "conflictResolution": {
                "conflictResolutionType": "Workspace",
                "conflictResolutionPolicy": "PreferRemote",
            },
            "options": {
                "allowOverrideItems": True,
            },
        }

        update_response = http.request(
            "POST",
            url=f"{FABRIC_BASE_URL}/workspaces/{workspace_id}/git/updateFromGit",
            headers=request_headers,
            body=json.dumps(update_body).encode("utf-8"),
        )

        try:
            formatted_update_response = json.loads(
                update_response.data.decode("utf-8")
            ) if update_response.data else {}
        except json.JSONDecodeError:
            formatted_update_response = {
                "raw": update_response.data.decode("utf-8", errors="replace")
            }

        pretty_print_json(
            "Resposta da API do Fabric - updateFromGit", formatted_update_response
        )

        if update_response.status == 202:
            print("Workspace sincronizada com sucesso!")
        else:
            print(
                f"Erro ao sincronizar workspace: HTTP {update_response.status} - "
                f"{formatted_update_response}"
            )
            sys.exit(1)
    else:
        print("Workspace já está sincronizada com o repositório Git.")


def main() -> None:
    workspace_id = get_env_or_exit("PBI_WORKSPACE_ID")
    connection_id = get_env_or_exit("PBI_CONNECTION_ID")

    authentication = {
        "tenant_id": get_env_or_exit("PBI_TENANT_ID"),
        "client_id": get_env_or_exit("PBI_CLIENT_ID"),
        "username": get_env_or_exit("PBI_USERNAME"),
        "password": get_env_or_exit("PBI_PASSWORD"),
    }

    sync_pbi_workspace(workspace_id, connection_id, authentication)


if __name__ == "__main__":
    main()