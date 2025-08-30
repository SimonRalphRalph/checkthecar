import { NextRequest, NextResponse } from 'next/server'
export function middleware(req: NextRequest) {
  if (req.nextUrl.pathname.startsWith('/admin')) {
    const auth = req.headers.get('authorization') || ''
    const [, hash] = auth.split(' ')
    const [u,p] = Buffer.from(hash || '', 'base64').toString().split(':')
    if (u === process.env.ADMIN_BASIC_AUTH_USER && p === process.env.ADMIN_BASIC_AUTH_PASS) return NextResponse.next()
    return new NextResponse('Unauthorized', { status: 401, headers:{'WWW-Authenticate':'Basic realm="admin"'} })
  }
}
export const config = { matcher: ['/admin/:path*'] }
