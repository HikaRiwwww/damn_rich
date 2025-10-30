import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'Damn Rich - 量化交易监控',
  description: '加密货币量化交易监控系统',
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="zh-CN">
      <body>{children}</body>
    </html>
  );
}

