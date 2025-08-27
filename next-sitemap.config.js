/** @type {import('next-sitemap').IConfig} */
module.exports = {
  siteUrl: 'https://checkthecar.example', // change this once you deploy
  generateRobotsTxt: true,
  exclude: ['/admin/*', '/api/*'],
}
