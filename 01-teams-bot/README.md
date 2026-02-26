# 01 — Teams Bot (Genie Conversation API)

Microsoft Teams bot that lets users ask natural-language questions against the
BMA Genie Space directly from any Teams channel or chat.

## Architecture

```
Teams User → Azure Bot Service → Azure Web App (app.py) → Genie Conversation API
```

## Azure Resources Required

1. **Resource Group** in a single region
2. **App Service Plan** (Linux, B1 minimum)
3. **Web App** (Python 3.12 runtime)
4. **Azure Bot AI Service** (Single Tenant)
5. **Databricks Service Principal** with CAN RUN on the Genie Space

## Deploy Steps

1. Create the Azure resources above (or use Terraform — see references below).
2. Copy `src/.env.example` → `src/.env` and fill in all values.
3. Set the Web App environment variables from `.env`.
4. Set the Bot messaging endpoint to `https://<webapp>.azurewebsites.net/api/messages`.
5. Add the Teams channel to the Bot.
6. Deploy the code:
   ```bash
   cd src/
   az login
   az webapp up --resource-group <rg> --name <webapp> --runtime "PYTHON:3.12" --sku B1
   ```
7. Install the bot in Teams (via App Studio or Teams Admin).

## Usage in Teams

- Type any question: *"What is the average CPI for Bermuda in 2023?"*
- Type `/reset` to start a fresh Genie conversation.
- Follow-up questions maintain context within the same thread.

## References

- [Databricks-BR/Genie_MS_Teams](https://github.com/Databricks-BR/Genie_MS_Teams)
- [Ryan Bates — Teams + Genie Guide](https://medium.com/@ryan-bates/microsoft-teams-meets-databricks-genie-api-a-complete-setup-guide-81f629ace634)
- [Official Docs — Connect agent to Teams](https://learn.microsoft.com/en-us/azure/databricks/generative-ai/agent-framework/teams-agent)
