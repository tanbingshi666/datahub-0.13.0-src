import React from "react";
import Layout from "@theme/Layout";
import useDocusaurusContext from "@docusaurus/useDocusaurusContext";
import SearchBar from "./_components/SearchBar";
import QuickLinkCards from "./_components/QuickLinkCards";
import GuideList from "./_components/GuideList";

import {
  FolderTwoTone,
  BookTwoTone,
  TagsTwoTone,
  ApiTwoTone,
  SearchOutlined,
  CompassTwoTone,
  NodeExpandOutlined,
  CheckCircleTwoTone,
  SafetyCertificateTwoTone,
  LockTwoTone,
  SlackOutlined,
  HistoryOutlined,
  InteractionOutlined,
  GlobalOutlined,
  FileTextOutlined,
} from "@ant-design/icons";

//quickLinkCards
import {
  ThunderboltTwoTone,
  DeploymentUnitOutlined,
  SyncOutlined,
  CodeTwoTone,
  QuestionCircleTwoTone,
  SlidersTwoTone,
  HeartTwoTone,
} from "@ant-design/icons";

const deploymentGuideContent = [
  {
    title: "Managed DataHub",
    platformIcon: "acryl",
    to: "docs/saas",
  },
  {
    title: "Docker",
    platformIcon: "docker",
    to: "docs/docker",
  },
  // {
  //   title: "AWS ECS",
  //   platformIcon: "amazon-ecs",
  //   to: "docs/deploy/aws",
  // },
  {
    title: "AWS",
    platformIcon: "amazon-eks",
    to: "docs/deploy/aws",
  },
  {
    title: "GCP",
    platformIcon: "google-cloud",
    to: "docs/deploy/gcp",
  },
];

const ingestionGuideContent = [
  {
    title: "Snowflake",
    platformIcon: "snowflake",
    to: "docs/generated/ingestion/sources/snowflake",
  },
  {
    title: "Looker",
    platformIcon: "looker",
    to: "docs/generated/ingestion/sources/looker",
  },
  {
    title: "Redshift",
    platformIcon: "redshift",
    to: "docs/generated/ingestion/sources/redshift",
  },
  {
    title: "Hive",
    platformIcon: "hive",
    to: "docs/generated/ingestion/sources/hive",
  },
  {
    title: "BigQuery",
    platformIcon: "bigquery",
    to: "docs/generated/ingestion/sources/bigquery",
  },
  {
    title: "dbt",
    platformIcon: "dbt",
    to: "docs/generated/ingestion/sources/dbt",
  },
  {
    title: "Athena",
    platformIcon: "athena",
    to: "docs/generated/ingestion/sources/athena",
  },
  {
    title: "PostgreSQL",
    platformIcon: "postgres",
    to: "docs/generated/ingestion/sources/postgres",
  },
];

const featureGuideContent = [
  { title: "Domains", icon: <FolderTwoTone />, to: "docs/domains" },
  {
    title: "Glossary Terms",
    icon: <BookTwoTone />,
    to: "docs/glossary/business-glossary",
  },
  { title: "Tags", icon: <TagsTwoTone />, to: "docs/tags" },
  {
    title: "Ingestion",
    icon: <ApiTwoTone />,
    to: "docs/ui-ingestion",
  },
  { title: "Search", icon: <SearchOutlined />, to: "docs/how/search" },
  // { title: "Browse", icon: <CompassTwoTone />, to: "/docs/quickstart" },
  {
    title: "Lineage Impact Analysis",
    icon: <NodeExpandOutlined />,
    to: "docs/act-on-metadata/impact-analysis",
  },
  {
    title: "Metadata Tests",
    icon: <CheckCircleTwoTone />,
    to: "docs/tests/metadata-tests",
  },
  {
    title: "Approval Flows",
    icon: <SafetyCertificateTwoTone />,
    to: "docs/managed-datahub/approval-workflows",
  },
  {
    title: "Personal Access Tokens",
    icon: <LockTwoTone />,
    to: "docs/authentication/personal-access-tokens",
  },
  {
    title: "Slack Notifications",
    icon: <SlackOutlined />,
    to: "docs/managed-datahub/saas-slack-setup",
  },
  {
    title: "Schema History",
    icon: <HistoryOutlined />,
    to: "docs/schema-history",
  },
];

const quickLinkContent = [
  {
    title: "Get Started",
    icon: <ThunderboltTwoTone />,
    description: "Details on how to get DataHub up and running",
    to: "/docs/quickstart",
  },
  {
    title: "Ingest Metadata",
    icon: <ApiTwoTone />,
    description: "Details on how to get Metadata loaded into DataHub",
    to: "/docs/metadata-ingestion",
  },
  {
    title: "API",
    icon: <DeploymentUnitOutlined />,
    description: "Details on how to utilize Metadata programmatically",
    to: "docs/api/datahub-apis",
  },
  {
    title: "Act on Metadata",
    icon: <SyncOutlined />,
    description: "Step-by-step guides for acting on Metadata Events",
    to: "docs/act-on-metadata",
  },
  {
    title: "Developer Guides",
    icon: <CodeTwoTone />,
    description: "Interact with DataHub programmatically",
    to: "/docs/api/datahub-apis",
  },
  {
    title: "Feature Guides",
    icon: <QuestionCircleTwoTone />,
    description: "Step-by-step guides for making the most of DataHub",
    to: "/docs/how/search",
  },
  {
    title: "Deployment Guides",
    icon: <SlidersTwoTone />,
    description: "Step-by-step guides for deploying DataHub to production",
    to: "/docs/deploy/aws",
  },
  {
    title: "Join the Community",
    icon: <HeartTwoTone />,
    description: "Collaborate, learn, and grow with us",
    to: "/docs/slack",
  },
];

const gitLinkContent = [
  {
    title: "datahub",
    icon: <ThunderboltTwoTone />,
    to: "https://github.com/datahub-project/datahub",
  },
  {
    title: "datahub-actions",
    icon: <ApiTwoTone />,
    to: "https://github.com/acryldata/datahub-actions",
  },
  {
    title: "datahub-helm",
    icon: <FileTextOutlined />,
    to: "https://github.com/acryldata/datahub-helm",
  },
  {
    title: "meta-world",
    icon: <GlobalOutlined />,
    to: "https://github.com/acryldata/meta-world",
  },
  {
    title: "business-glossary-sync-action",
    icon: <InteractionOutlined />,
    to: "https://github.com/acryldata/business-glossary-sync-action",
  },
  {
    title: "dbt-impact-action",
    icon: <NodeExpandOutlined />,
    to: "https://github.com/acryldata/dbt-impact-action",
  },
];

function Docs() {
  const context = useDocusaurusContext();
  const { siteConfig = {} } = context;

  return (
    <Layout
      title={siteConfig.tagline}
      description="DataHub is a data discovery application built on an extensible metadata platform that helps you tame the complexity of diverse data ecosystems."
    >
      <header className={"hero"}>
        <div className="container">
          <div className="hero__content">
            <div>
              <h1 className="hero__title">Documentation</h1>
              <p className="hero__subtitle">
                Guides and tutorials for everything DataHub.
              </p>
              <SearchBar />
            </div>
          </div>
          <QuickLinkCards quickLinkContent={quickLinkContent} />
          <GuideList
            title="Deployment Guides"
            content={deploymentGuideContent}
          />
          <GuideList
            title="Ingestion Guides"
            content={ingestionGuideContent}
            seeMoreLink={{ label: "See all 50+ sources", to: "/integrations" }}
          />
          <GuideList
            title="Feature Guides"
            content={featureGuideContent}
            seeMoreLink={{ label: "See all guides", to: "/docs/how/search" }}
          />
          <GuideList
            title="Github Repositories"
            content={gitLinkContent}
            seeMoreLink={{
              label: "See all repositories",
              to: "https://github.com/datahub-project/datahub#source-code-and-repositories",
            }}
          />
        </div>
      </header>
    </Layout>
  );
}

export default Docs;
