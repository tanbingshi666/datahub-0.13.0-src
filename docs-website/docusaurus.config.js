require("dotenv").config();
const isSaas = process.env.DOCUSAURUS_IS_SAAS === "true";

module.exports = {
  title: process.env.DOCUSAURUS_CONFIG_TITLE || "DataHub",
  tagline: "A Metadata Platform for the Modern Data Stack",
  url: process.env.DOCUSAURUS_CONFIG_URL || "https://datahubproject.io",
  baseUrl: process.env.DOCUSAURUS_CONFIG_BASE_URL || "/",
  onBrokenLinks: "throw",
  onBrokenMarkdownLinks: "throw",
  favicon: "img/favicon.ico",
  organizationName: "datahub-project", // Usually your GitHub org/user name.
  projectName: "datahub", // Usually your repo name.
  staticDirectories: ["static", "genStatic"],
  stylesheets: ["https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;700&display=swap"],
  scripts: [
    {
      src: "https://tools.luckyorange.com/core/lo.js?site-id=28ea8a38",
      async: true,
      defer: true,
    },
  ],
  noIndex: isSaas,
  customFields: {
    isSaas: isSaas,
    markpromptProjectKey: process.env.DOCUSAURUS_MARKPROMPT_PROJECT_KEY || "IeF3CUFCUQWuouZ8MP5Np9nES52QAtaA",
  },
  themeConfig: {
    ...(!isSaas && {
      announcementBar: {
        id: "announcement",
        content:
          '<div><img src="/img/acryl-logo-white-mark.svg" /><p><strong>Managed DataHub</strong><span> &nbsp;Acryl Data delivers an easy to consume DataHub platform for the enterprise</span></p></div> <a href="https://www.acryldata.io/datahub-sign-up?utm_source=datahub&utm_medium=referral&utm_campaign=acryl_signup" target="_blank" class="button button--primary">Sign up for Managed DataHub&nbsp;→</a>',
        backgroundColor: "#070707",
        textColor: "#ffffff",
        isCloseable: false,
      },
    }),
    navbar: {
      title: null,
      logo: {
        alt: "DataHub Logo",
        src: `img/${isSaas ? "acryl" : "datahub"}-logo-color-light-horizontal.svg`,
        srcDark: `img/${isSaas ? "acryl" : "datahub"}-logo-color-dark-horizontal.svg`,
      },
      items: [
        {
          to: "docs/",
          activeBasePath: "docs",
          label: "Docs",
          position: "right",
        },
        {
          to: "/integrations",
          activeBasePath: "integrations",
          label: "Integrations",
          position: "right",
        },
        {
          type: "dropdown",
          label: "Community",
          position: "right",
          items: [
            {
              to: "/slack",
              label: "Join Slack",
            },
            {
              to: "/events",
              label: "Events",
            },
            {
              to: "/champions",
              label: "Champions",
            },
          ],
        },
        {
          type: "dropdown",
          label: "Resources",
          position: "right",
          items: [
            {
              href: "https://demo.datahubproject.io/",
              label: "Demo",
            },
            {
              href: "https://www.acryldata.io/blog",
              label: "Blog",
            },
            {
              href: "https://feature-requests.datahubproject.io/roadmap",
              label: "Roadmap",
            },
            {
              href: "https://github.com/datahub-project/datahub",
              label: "GitHub",
            },
            {
              href: "https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w",
              label: "YouTube",
            },
            {
              href: "https://www.youtube.com/playlist?list=PLdCtLs64vZvGCKMQC2dJEZ6cUqWsREbFi",
              label: "Case Studies",
            },
            {
              href: "https://www.youtube.com/playlist?list=PLdCtLs64vZvErAXMiqUYH9e63wyDaMBgg",
              label: "DataHub Basics",
            },
          ],
        },
        {
          type: "docsVersionDropdown",
          position: "left",
          dropdownActiveClassDisabled: true,
        },
      ],
    },
    footer: {
      style: "dark",
      links: [
        {
          title: "Docs",
          items: [
            {
              label: "Introduction",
              to: "docs/",
            },
            {
              label: "Quickstart",
              to: "docs/quickstart",
            },
            {
              label: "Features",
              to: "docs/features",
            },
          ],
        },
        {
          title: "Community",
          items: [
            {
              label: "Slack",
              href: "https://slack.datahubproject.io",
            },
            {
              label: "YouTube",
              href: "https://www.youtube.com/channel/UC3qFQC5IiwR5fvWEqi_tJ5w",
            },
            {
              label: "Blog",
              href: "https://blog.datahubproject.io/",
            },
            {
              label: "Town Halls",
              to: "docs/townhalls",
            },
            {
              label: "Adoption",
              to: "docs/#adoption",
            },
          ],
        },
        {
          title: "More",
          items: [
            {
              label: "Demo",
              to: "https://demo.datahubproject.io/",
            },
            {
              label: "Roadmap",
              href: "https://feature-requests.datahubproject.io/roadmap",
            },
            {
              label: "Contributing",
              to: "docs/contributing",
            },
            {
              label: "GitHub",
              href: "https://github.com/datahub-project/datahub",
            },
            {
              label: "Feature Requests",
              href: "https://feature-requests.datahubproject.io/",
            },
          ],
        },
      ],
      copyright: `Copyright © 2015-${new Date().getFullYear()} DataHub Project Authors.`,
    },
    prism: {
      // https://docusaurus.io/docs/markdown-features/code-blocks#theming
      // theme: require("prism-react-renderer/themes/vsLight"),
      // darkTheme: require("prism-react-renderer/themes/vsDark"),
      additionalLanguages: ["ini", "java", "graphql", "shell-session"],
    },
    algolia: {
      appId: "RK0UG797F3",
      apiKey: "39d7eb90d8b31d464e309375a52d674f",
      indexName: "datahubproject",
      insights: true,
      contextualSearch: true,
      // debug: true,
    },
  },
  presets: [
    [
      "@docusaurus/preset-classic",
      {
        docs: {
          path: "genDocs",
          sidebarPath: require.resolve("./sidebars.js"),
          ...(!isSaas && {
            editUrl: "https://github.com/datahub-project/datahub/blob/master/",
          }),
          numberPrefixParser: false,
          // TODO: make these work correctly with the doc generation
          showLastUpdateAuthor: true,
          showLastUpdateTime: true,
        },
        blog: false,
        theme: {
          customCss: [
            isSaas ? require.resolve("./src/styles/acryl.scss") : require.resolve("./src/styles/datahub.scss"),
            require.resolve("./src/styles/global.scss"),
            require.resolve("./src/styles/sphinx.scss"),
            require.resolve("./src/styles/config-table.scss"),
          ],
        },
        pages: {
          path: "src/pages",
          mdxPageComponent: "@theme/MDXPage",
        },
      },
    ],
  ],
  plugins: [
    ["@docusaurus/plugin-ideal-image", { quality: 100, sizes: [320, 640, 1280, 1440, 1600] }],
    "docusaurus-plugin-sass",
    [
      "docusaurus-graphql-plugin",
      {
        schema: "./graphql/combined.graphql",
        routeBasePath: "/docs/graphql",
      },
    ],
    // '@docusaurus/plugin-google-gtag',
    // [
    //   require.resolve("@easyops-cn/docusaurus-search-local"),
    //   {
    //     // `hashed` is recommended as long-term-cache of index file is possible.
    //     hashed: true,
    //     language: ["en"],
    //     docsDir: "genDocs",
    //     blogDir: [],
    //   },
    // ],
  ],
};
