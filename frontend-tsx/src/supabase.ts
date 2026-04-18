import { createClient } from '@supabase/supabase-js';

const SUPABASE_URL = 'https://jmexnqbrwrzxbfysrleh.supabase.co';
const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImptZXhucWJyd3J6eGJmeXNybGVoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzU0OTM4MDEsImV4cCI6MjA5MTA2OTgwMX0.9O5qLz1dQZhrKdESgKyMbr21OvHKD944tRoZl-1SgYI';

export const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

export async function getAuthHeaders(): Promise<Record<string, string>> {
  const { data } = await supabase.auth.getSession();
  const token = data.session?.access_token;
  if (!token) return {};
  return { Authorization: `Bearer ${token}` };
}
