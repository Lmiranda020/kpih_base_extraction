import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def _badge_api(nome: str, sucesso: bool) -> str:
    cor  = "#2e7d32" if sucesso else "#c62828"
    icon = "✅" if sucesso else "❌"
    return (
        f"<tr>"
        f"<td style='padding:6px 10px;font-size:13px;color:#333;border-bottom:1px solid #f0f0f0'>"
        f"  {icon} {nome}"
        f"</td>"
        f"<td style='padding:6px 10px;font-size:13px;font-weight:bold;color:{cor};"
        f"border-bottom:1px solid #f0f0f0;text-align:center'>"
        f"  {'Sucesso' if sucesso else 'Erro'}"
        f"</td>"
        f"</tr>"
    )


def _html_competencias(competencias: list) -> str:
    if not competencias:
        return "<p style='color:#888;margin:0;font-size:13px'>Nenhuma competência nova processada.</p>"
    itens = "".join(
        f"<span style='display:inline-block;background:#1565c0;color:#fff;"
        f"border-radius:4px;padding:3px 10px;margin:3px;font-size:13px'>📅 {c}</span>"
        for c in competencias
    )
    return f"<div style='line-height:2.2'>{itens}</div>"


def _montar_html_copia() -> str:
    """HTML simplificado para o modo cópia (nada novo)."""
    return """
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#f4f6f8;padding:0;margin:0">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6f8;padding:30px 0">
    <tr><td align="center">
      <table width="620" cellpadding="0" cellspacing="0"
             style="background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.12);overflow:hidden">

        <tr>
          <td style="background:#1F4E79;padding:24px 32px">
            <h1 style="color:#fff;margin:0;font-size:22px">🤖 Extração Incremental de Dados</h1>
            <p style="color:#a8c8e8;margin:6px 0 0;font-size:14px">Relatório automático de execução</p>
          </td>
        </tr>

        <tr>
          <td style="padding:28px 32px">
            <div style="border-left:5px solid #1565c0;background:#e3f2fd;padding:16px 20px;border-radius:4px">
              <p style="margin:0;font-size:18px;font-weight:bold;color:#1565c0">
                ℹ️ NENHUMA COMPETÊNCIA NOVA
              </p>
              <p style="margin:8px 0 0;color:#555;font-size:13px">
                Todas as competências disponíveis já foram processadas anteriormente.
                Nenhuma extração foi necessária nesta execução.
              </p>
            </div>
          </td>
        </tr>

        <tr>
          <td style="padding:0 32px 28px">
            <p style="margin:0;font-size:12px;color:#999;text-align:center">
              Este é um e-mail automático — não responda.
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>
"""


def _montar_html(resumo: dict) -> str:
    """Monta o corpo HTML completo com todas as informações da execução."""

    total       = resumo.get("total_apis", 0)
    n_ok        = resumo.get("apis_sucesso", 0)
    n_err       = resumo.get("apis_erro", 0)
    duracao     = resumo.get("duracao_total", "-")
    registros   = resumo.get("total_registros", 0)
    competencias = resumo.get("competencias_processadas", [])
    apis        = resumo.get("apis_detalhes", {})   # {nome: bool(sucesso)}
    inicio      = resumo.get("data_inicio", "-")
    fim         = resumo.get("data_fim", "-")
    status      = resumo.get("status_final", "")
    observacoes = resumo.get("observacoes", "")

    pct_ok    = round(n_ok / total * 100) if total else 0
    cor_status = "#2e7d32" if status == "Sucesso Total" else (
                 "#f57c00" if status == "Parcial" else "#b71c1c")
    txt_status = (f"CONCLUÍDA COM SUCESSO ✅" if status == "Sucesso Total" else
                  f"CONCLUÍDA PARCIALMENTE ⚠️" if status == "Parcial" else
                  f"FALHA NA EXECUÇÃO ❌")

    barra_ok  = f"<div style='width:{pct_ok}%;background:#43a047;height:100%;border-radius:4px'></div>"
    barra_err = f"<div style='width:{100-pct_ok}%;background:#e53935;height:100%;'></div>"

    linhas_apis = "".join(_badge_api(nome, ok) for nome, ok in apis.items())
    bloco_competencias = _html_competencias(competencias)
    bloco_obs = (
        f"<tr><td style='padding:16px 32px 0'>"
        f"<p style='margin:0;font-size:13px;color:#555'><strong>📝 Observações:</strong> {observacoes}</p>"
        f"</td></tr>"
        if observacoes else ""
    )

    return f"""
<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"></head>
<body style="font-family:Arial,sans-serif;background:#f4f6f8;padding:0;margin:0">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#f4f6f8;padding:30px 0">
    <tr><td align="center">
      <table width="620" cellpadding="0" cellspacing="0"
             style="background:#fff;border-radius:8px;box-shadow:0 2px 8px rgba(0,0,0,.12);overflow:hidden">

        <!-- Cabeçalho -->
        <tr>
          <td style="background:#1F4E79;padding:24px 32px">
            <h1 style="color:#fff;margin:0;font-size:22px">🤖 Extração Incremental de Dados</h1>
            <p style="color:#a8c8e8;margin:6px 0 0;font-size:14px">Relatório automático de execução</p>
          </td>
        </tr>

        <!-- Status geral -->
        <tr>
          <td style="padding:20px 32px 0">
            <div style="border-left:5px solid {cor_status};background:#fafafa;
                        padding:14px 18px;border-radius:4px">
              <p style="margin:0;font-size:18px;font-weight:bold;color:{cor_status}">{txt_status}</p>
            </div>
          </td>
        </tr>

        <!-- Métricas -->
        <tr>
          <td style="padding:20px 32px 0">
            <table width="100%" cellspacing="8" cellpadding="0">
              <tr>
                <td width="22%" align="center"
                    style="background:#e3f2fd;border-radius:6px;padding:14px">
                  <div style="font-size:26px;font-weight:bold;color:#1565c0">{total}</div>
                  <div style="font-size:11px;color:#555;margin-top:4px">APIs Executadas</div>
                </td>
                <td width="4%"></td>
                <td width="22%" align="center"
                    style="background:#e8f5e9;border-radius:6px;padding:14px">
                  <div style="font-size:26px;font-weight:bold;color:#2e7d32">{n_ok}</div>
                  <div style="font-size:11px;color:#555;margin-top:4px">Sucesso</div>
                </td>
                <td width="4%"></td>
                <td width="22%" align="center"
                    style="background:#ffebee;border-radius:6px;padding:14px">
                  <div style="font-size:26px;font-weight:bold;color:#c62828">{n_err}</div>
                  <div style="font-size:11px;color:#555;margin-top:4px">Com Erro</div>
                </td>
                <td width="4%"></td>
                <td width="22%" align="center"
                    style="background:#f3e5f5;border-radius:6px;padding:14px">
                  <div style="font-size:26px;font-weight:bold;color:#6a1b9a">{pct_ok}%</div>
                  <div style="font-size:11px;color:#555;margin-top:4px">Taxa Sucesso</div>
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- Barra de progresso -->
        <tr>
          <td style="padding:14px 32px 0">
            <div style="height:10px;background:#eee;border-radius:4px;overflow:hidden;display:flex">
              {barra_ok}{barra_err}
            </div>
          </td>
        </tr>

        <!-- Período + métricas extras -->
        <tr>
          <td style="padding:16px 32px 0">
            <table width="100%" style="background:#f9f9f9;border-radius:6px;border:1px solid #e0e0e0">
              <tr>
                <td style="padding:10px 16px;font-size:13px;color:#333">
                  🕐 <strong>Início:</strong> {inicio}
                </td>
                <td style="padding:10px 16px;font-size:13px;color:#333">
                  🕑 <strong>Fim:</strong> {fim}
                </td>
              </tr>
              <tr>
                <td style="padding:10px 16px;font-size:13px;color:#333">
                  ⏱️ <strong>Duração:</strong> {duracao}
                </td>
                <td style="padding:10px 16px;font-size:13px;color:#333">
                  📦 <strong>Registros extraídos:</strong> {registros:,}
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- Competências processadas -->
        <tr>
          <td style="padding:20px 32px 0">
            <h3 style="margin:0 0 8px;color:#1565c0;font-size:14px">
              📅 COMPETÊNCIAS PROCESSADAS ({len(competencias)})
            </h3>
            <div style="border:1px solid #bbdefb;background:#e3f2fd;
                        border-radius:6px;padding:12px">
              {bloco_competencias}
            </div>
          </td>
        </tr>

        <!-- Detalhe por API -->
        <tr>
          <td style="padding:20px 32px 0">
            <h3 style="margin:0 0 8px;color:#333;font-size:14px">🔌 STATUS POR API</h3>
            <table width="100%"
                   style="border-collapse:collapse;border:1px solid #e0e0e0;border-radius:6px;overflow:hidden">
              <tr style="background:#f5f5f5">
                <th style="padding:8px 10px;font-size:12px;color:#555;text-align:left;
                           border-bottom:2px solid #e0e0e0">API</th>
                <th style="padding:8px 10px;font-size:12px;color:#555;text-align:center;
                           border-bottom:2px solid #e0e0e0;width:100px">Status</th>
              </tr>
              {linhas_apis}
            </table>
          </td>
        </tr>

        {bloco_obs}

        <!-- Rodapé -->
        <tr>
          <td style="padding:24px 32px;border-top:1px solid #e0e0e0;margin-top:20px">
            <p style="margin:0;font-size:12px;color:#999;text-align:center">
              📎 O log completo está anexado neste e-mail.<br>
              Este é um e-mail automático — não responda.
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>
"""


def enviar_email_extracao(resumo: dict) -> bool:
    """
    Envia e-mail com o resultado da extração incremental via Gmail (App Password).

    Args:
        resumo: dicionário montado pelo main.py com os dados da execução.
                Campos esperados:
                  - modo              : 'processar' ou 'copiar'
                  - status_final      : 'Sucesso Total' | 'Parcial' | 'Falha'
                  - total_apis        : int
                  - apis_sucesso      : int
                  - apis_erro         : int
                  - duracao_total     : str  (ex: "2m 34s")
                  - total_registros   : int
                  - data_inicio       : str
                  - data_fim          : str
                  - competencias_processadas : list[str]
                  - apis_detalhes     : dict {nome_api: bool(sucesso)}
                  - observacoes       : str  (opcional)
                  - caminho_log       : str  (caminho do Excel de log para anexar)

    Returns:
        True se enviado com sucesso, False caso contrário.
    """
    remetente    = os.getenv("GMAIL_REMETENTE")
    app_senha    = os.getenv("GMAIL_APP_SENHA")
    destinatario = os.getenv("EMAIL_DESTINATARIO", "")

    if not all([remetente, app_senha, destinatario]):
        print("⚠️  Variáveis de e-mail não configuradas no .env "
              "(GMAIL_REMETENTE, GMAIL_APP_SENHA, EMAIL_DESTINATARIO)")
        return False

    destinatarios = [d.strip() for d in destinatario.split(",") if d.strip()]

    modo       = resumo.get("modo", "processar")
    n_err      = resumo.get("apis_erro", 0)
    status     = resumo.get("status_final", "")

    # Assunto do e-mail
    if modo == "copiar":
        assunto = "Extração Incremental — ℹ️ Nenhuma competência nova"
    elif status == "Sucesso Total":
        assunto = "Extração Incremental — ✅ Concluída com sucesso"
    elif status == "Parcial":
        assunto = f"Extração Incremental — ⚠️ Concluída com {n_err} erro(s)"
    else:
        assunto = "Extração Incremental — ❌ Falha na execução"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = assunto
    msg["From"]    = remetente
    msg["To"]      = ", ".join(destinatarios)

    # Corpo HTML
    html = _montar_html_copia() if modo == "copiar" else _montar_html(resumo)
    msg.attach(MIMEText(html, "html", "utf-8"))

    # Anexar log Excel se existir
    caminho_log = resumo.get("caminho_log", "")
    if caminho_log and os.path.exists(caminho_log):
        try:
            with open(caminho_log, "rb") as f:
                parte = MIMEBase("application",
                                 "vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                parte.set_payload(f.read())
            encoders.encode_base64(parte)
            nome_anexo = os.path.basename(caminho_log)
            parte.add_header("Content-Disposition",
                             f'attachment; filename="{nome_anexo}"')
            msg.attach(parte)
            print(f"📎 Anexo adicionado: {nome_anexo}")
        except Exception as e:
            print(f"⚠️  Erro ao anexar log: {e}")

    # Envio
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as servidor:
            servidor.login(remetente, app_senha)
            servidor.sendmail(remetente, destinatarios, msg.as_string())
        print(f"📧 E-mail enviado para: {', '.join(destinatarios)}")
        return True
    except smtplib.SMTPAuthenticationError:
        print("❌ Falha de autenticação SMTP. Verifique GMAIL_APP_SENHA no .env.")
    except smtplib.SMTPException as e:
        print(f"❌ Erro SMTP ao enviar e-mail: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado ao enviar e-mail: {e}")
    return False