/** @type {import('next-sitemap').IConfig} */
module.exports = {
  siteUrl: 'https://checkthecar.example',
  generateRobotsTxt: true,
  exclude: ['/admin/*', '/api/*']
}
