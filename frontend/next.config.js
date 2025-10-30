/** @type {import('next').NextConfig} */
const nextConfig = {
  // API 代理配置 - 统一 API 服务
  async rewrites() {
    return [
      {
        source: '/api/data-sync/:path*',
        destination: 'http://api:8000/api/data-sync/:path*',
      },
      {
        source: '/api/trading-bot/:path*',
        destination: 'http://api:8000/api/trading-bot/:path*',
      },
    ];
  },
};

module.exports = nextConfig;

