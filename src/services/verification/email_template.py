"""
邮件模板
提供验证码邮件的 HTML 和纯文本模板
"""

from typing import Dict


class EmailTemplate:
    """邮件模板类"""

    @staticmethod
    def get_verification_code_html(code: str, expire_minutes: int = 30, **kwargs) -> str:
        """
        获取验证码邮件 HTML 模板

        Args:
            code: 验证码
            expire_minutes: 过期时间（分钟）
            **kwargs: 其他模板变量

        Returns:
            HTML 邮件内容
        """
        app_name = kwargs.get("app_name", "Aether")
        support_email = kwargs.get("support_email", "")

        html = f"""
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>邮箱验证码</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            background-color: #f5f5f5;
            margin: 0;
            padding: 0;
        }}
        .container {{
            max-width: 600px;
            margin: 40px auto;
            background-color: #ffffff;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: #ffffff;
            padding: 30px;
            text-align: center;
        }}
        .header h1 {{
            margin: 0;
            font-size: 28px;
            font-weight: 600;
        }}
        .content {{
            padding: 40px 30px;
        }}
        .greeting {{
            font-size: 18px;
            margin-bottom: 20px;
            color: #333;
        }}
        .message {{
            font-size: 16px;
            color: #666;
            margin-bottom: 30px;
            line-height: 1.8;
        }}
        .code-container {{
            background-color: #f8f9fa;
            border: 2px dashed #667eea;
            border-radius: 8px;
            padding: 30px;
            text-align: center;
            margin: 30px 0;
        }}
        .code-label {{
            font-size: 14px;
            color: #666;
            margin-bottom: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}
        .code {{
            font-size: 36px;
            font-weight: 700;
            color: #667eea;
            letter-spacing: 8px;
            font-family: 'Courier New', Courier, monospace;
            margin: 10px 0;
        }}
        .expire-info {{
            font-size: 14px;
            color: #999;
            margin-top: 10px;
        }}
        .warning {{
            background-color: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            margin: 20px 0;
            font-size: 14px;
            color: #856404;
        }}
        .footer {{
            background-color: #f8f9fa;
            padding: 20px 30px;
            text-align: center;
            font-size: 14px;
            color: #999;
        }}
        .footer a {{
            color: #667eea;
            text-decoration: none;
        }}
        .divider {{
            height: 1px;
            background-color: #e9ecef;
            margin: 30px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>{app_name}</h1>
        </div>
        <div class="content">
            <div class="greeting">您好！</div>
            <div class="message">
                感谢您注册 {app_name}。为了验证您的邮箱地址，请使用以下验证码完成注册流程：
            </div>

            <div class="code-container">
                <div class="code-label">验证码</div>
                <div class="code">{code}</div>
                <div class="expire-info">
                    验证码有效期：{expire_minutes} 分钟
                </div>
            </div>

            <div class="warning">
                <strong>安全提示：</strong>
                <ul style="margin: 10px 0; padding-left: 20px;">
                    <li>请勿将此验证码透露给任何人</li>
                    <li>如果您没有请求此验证码，请忽略此邮件</li>
                    <li>验证码在 {expire_minutes} 分钟后自动失效</li>
                </ul>
            </div>

            <div class="divider"></div>

            <div class="message" style="font-size: 14px;">
                如果您在注册过程中遇到任何问题，请随时联系我们的支持团队。
            </div>
        </div>
        <div class="footer">
            <p>此邮件由系统自动发送，请勿直接回复。</p>
            {f'<p>需要帮助？联系我们：<a href="mailto:{support_email}">{support_email}</a></p>' if support_email else ''}
            <p>&copy; {app_name}. All rights reserved.</p>
        </div>
    </div>
</body>
</html>
        """
        return html.strip()

    @staticmethod
    def get_verification_code_text(code: str, expire_minutes: int = 30, **kwargs) -> str:
        """
        获取验证码邮件纯文本模板

        Args:
            code: 验证码
            expire_minutes: 过期时间（分钟）
            **kwargs: 其他模板变量

        Returns:
            纯文本邮件内容
        """
        app_name = kwargs.get("app_name", "Aether")
        support_email = kwargs.get("support_email", "")

        text = f"""
{app_name} - 邮箱验证码
{'=' * 50}

您好！

感谢您注册 {app_name}。为了验证您的邮箱地址，请使用以下验证码完成注册流程：

验证码：{code}

验证码有效期：{expire_minutes} 分钟

{'=' * 50}

安全提示：
- 请勿将此验证码透露给任何人
- 如果您没有请求此验证码，请忽略此邮件
- 验证码在 {expire_minutes} 分钟后自动失效

{'=' * 50}

如果您在注册过程中遇到任何问题，请随时联系我们的支持团队。
{f'联系邮箱：{support_email}' if support_email else ''}

此邮件由系统自动发送，请勿直接回复。

&copy; {app_name}. All rights reserved.
        """
        return text.strip()

    @staticmethod
    def get_subject(template_type: str = "verification") -> str:
        """
        获取邮件主题

        Args:
            template_type: 模板类型

        Returns:
            邮件主题
        """
        subjects = {
            "verification": "邮箱验证码 - 请完成验证",
            "welcome": "欢迎加入 Aether",
            "password_reset": "密码重置验证码",
        }
        return subjects.get(template_type, "Aether 通知")
