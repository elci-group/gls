//! The provider catalogue.
//!
//! # On "top 20 by market share"
//!
//! There is no public, real-time feed of inference-provider market share —
//! anyone claiming to poll one live would be fabricating a data source. This
//! list is instead a curated, editorially-ordered snapshot of the platforms
//! most commonly cited as major LLM inference providers as of this writing:
//! the frontier labs, the hyperscaler platforms that host them, and the
//! specialised high-throughput inference vendors. Treat the ordering as
//! "roughly how prominent this provider is," not a sourced ranking.
//!
//! # On live polling
//!
//! What *is* real: for any provider whose [`ProviderKind`] is not
//! [`ProviderKind::Unsupported`], [`crate::poll::poll_models`] makes an
//! actual authenticated HTTP request to that provider's model-listing
//! endpoint. Providers whose only auth scheme is something heavier than a
//! bearer/API-key header (AWS SigV4, GCP/Azure OAuth, Replicate's
//! versioned-model registry, …) are marked `Unsupported` with the reason —
//! the menu still lists them, but falls back to manual model-id entry
//! rather than pretending to poll something it can't.

/// One entry in the provider catalogue.
#[derive(Debug, Clone, Copy)]
pub struct Provider {
    /// Stable identifier, used as the cache key and in `--provider` args.
    pub id: &'static str,
    /// Human-readable display name.
    pub name: &'static str,
    /// Environment variable this provider's API key is read from.
    pub env_var: &'static str,
    pub kind: ProviderKind,
}

/// How (or whether) this provider's model list can be live-polled.
#[derive(Debug, Clone, Copy)]
pub enum ProviderKind {
    /// `GET {base}/v1/models` with `Authorization: Bearer <key>`, response
    /// shaped `{"data": [{"id": "..."}, ...]}` — the OpenAI wire format
    /// that most inference platforms have converged on.
    OpenAiCompatible { base: &'static str },
    /// Anthropic's `GET {base}/v1/models`: `x-api-key` + `anthropic-version`
    /// headers, same `{"data": [{"id": ...}]}` response shape.
    Anthropic { base: &'static str },
    /// Cohere's `GET {base}/v1/models`: bearer auth, response shaped
    /// `{"models": [{"name": "..."}]}`.
    Cohere { base: &'static str },
    /// No simple bearer-token REST listing is available (OAuth token
    /// exchange, AWS SigV4, a non-flat model registry, …). The menu falls
    /// back to manual model-id entry for these.
    Unsupported { reason: &'static str },
}

/// The catalogue, in roughly-descending order of prominence. See the module
/// doc comment for what that ordering does and doesn't mean.
pub const PROVIDERS: &[Provider] = &[
    Provider {
        id: "openai",
        name: "OpenAI",
        env_var: "OPENAI_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.openai.com",
        },
    },
    Provider {
        id: "anthropic",
        name: "Anthropic",
        env_var: "ANTHROPIC_API_KEY",
        kind: ProviderKind::Anthropic {
            base: "https://api.anthropic.com",
        },
    },
    Provider {
        id: "google",
        name: "Google (Gemini API / Vertex AI)",
        env_var: "GOOGLE_API_KEY",
        kind: ProviderKind::Unsupported {
            reason: "Gemini API uses a query-string key with a non-OpenAI response shape; Vertex AI requires GCP OAuth",
        },
    },
    Provider {
        id: "azure-openai",
        name: "Microsoft Azure OpenAI Service",
        env_var: "AZURE_OPENAI_API_KEY",
        kind: ProviderKind::Unsupported {
            reason: "per-resource endpoint + api-version, auth via key or Azure AD — no fixed base URL to poll",
        },
    },
    Provider {
        id: "bedrock",
        name: "Amazon Bedrock",
        env_var: "AWS_ACCESS_KEY_ID",
        kind: ProviderKind::Unsupported {
            reason: "requires AWS SigV4-signed requests, not a bearer token",
        },
    },
    Provider {
        id: "meta",
        name: "Meta (Llama API)",
        env_var: "LLAMA_API_KEY",
        kind: ProviderKind::Unsupported {
            reason: "no stable public model-listing endpoint",
        },
    },
    Provider {
        id: "xai",
        name: "xAI (Grok)",
        env_var: "XAI_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.x.ai",
        },
    },
    Provider {
        id: "groq",
        name: "Groq",
        env_var: "GROQ_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.groq.com/openai",
        },
    },
    Provider {
        id: "mistral",
        name: "Mistral AI",
        env_var: "MISTRAL_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.mistral.ai",
        },
    },
    Provider {
        id: "deepseek",
        name: "DeepSeek",
        env_var: "DEEPSEEK_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.deepseek.com",
        },
    },
    Provider {
        id: "together",
        name: "Together AI",
        env_var: "TOGETHER_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.together.xyz",
        },
    },
    Provider {
        id: "fireworks",
        name: "Fireworks AI",
        env_var: "FIREWORKS_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.fireworks.ai/inference",
        },
    },
    Provider {
        id: "perplexity",
        name: "Perplexity",
        env_var: "PERPLEXITY_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.perplexity.ai",
        },
    },
    Provider {
        id: "cohere",
        name: "Cohere",
        env_var: "COHERE_API_KEY",
        kind: ProviderKind::Cohere {
            base: "https://api.cohere.com",
        },
    },
    Provider {
        id: "openrouter",
        name: "OpenRouter",
        env_var: "OPENROUTER_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://openrouter.ai/api",
        },
    },
    Provider {
        id: "cerebras",
        name: "Cerebras",
        env_var: "CEREBRAS_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.cerebras.ai",
        },
    },
    Provider {
        id: "sambanova",
        name: "SambaNova",
        env_var: "SAMBANOVA_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.sambanova.ai",
        },
    },
    Provider {
        id: "watsonx",
        name: "IBM watsonx.ai",
        env_var: "WATSONX_API_KEY",
        kind: ProviderKind::Unsupported {
            reason: "requires IAM OAuth token exchange",
        },
    },
    Provider {
        id: "replicate",
        name: "Replicate",
        env_var: "REPLICATE_API_TOKEN",
        kind: ProviderKind::Unsupported {
            reason: "versioned model registry, not a flat bearer-token model list",
        },
    },
    Provider {
        id: "anyscale",
        name: "Anyscale Endpoints",
        env_var: "ANYSCALE_API_KEY",
        kind: ProviderKind::OpenAiCompatible {
            base: "https://api.endpoints.anyscale.com",
        },
    },
];
