/** @type {import('next').NextConfig} */
const nextConfig = {
  redirects: async () => [
    {
      source: "/",
      destination: "/dashboard/ingestion",
      permanent: false,
    },
    {
      source: "/dashboard",
      destination: "/dashboard/ingestion",
      permanent: false,
    },
  ],
};

export default nextConfig;
