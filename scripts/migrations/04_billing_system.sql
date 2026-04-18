-- 04_billing_system.sql
-- Enforces ShikiPond billing logic, storage quotas, and compute tracking.

-- 1. Create Billing Profiles Ledger
CREATE TABLE IF NOT EXISTS public.billing_profiles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  owner_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE,
  plan_tier VARCHAR NOT NULL DEFAULT 'starter', -- 'starter', 'pro', 'team'
  storage_limit_bytes BIGINT NOT NULL DEFAULT 53687091200, -- 50GB limit default
  compute_credit_balance DECIMAL NOT NULL DEFAULT 0.00,
  compute_rate DECIMAL NOT NULL DEFAULT 0.028, -- $0.028 per 60s
  trial_ends_at TIMESTAMP WITH TIME ZONE,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
  updated_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- RLS Enforcement
ALTER TABLE public.billing_profiles ENABLE ROW LEVEL SECURITY;

-- Select policy: User can read their own billing profile synchronously
CREATE POLICY "Users can view their own billing profile"
  ON public.billing_profiles
  FOR SELECT
  USING (auth.uid() = owner_id);

-- Restrict mutations to service role/backend systems so users cannot artificially inflate balance
CREATE POLICY "Only service_role can mutate billing profiles"
  ON public.billing_profiles
  FOR ALL
  USING (current_setting('request.jwt.claims', true)::json->>'role' = 'service_role');


-- 2. Setup Trial Automation Trigger
-- When a user hits the /signup route, hook into Supabase Auth and automatically generate a tracked profile.
CREATE OR REPLACE FUNCTION public.handle_new_user_billing()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO public.billing_profiles (
    owner_id,
    plan_tier,
    storage_limit_bytes,
    compute_credit_balance,
    compute_rate,
    trial_ends_at
  )
  VALUES (
    new.id,
    'starter',
    53687091200, -- 50 GB
    2.00,        -- Provide $2.00 of hardcap trial credits (~70 queries) to prevent compute abuse over 7 days
    0.028,       -- Individual compute rate
    now() + interval '7 days'
  );
  RETURN new;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

-- Bind the trigger (Use DROP first to ensure idempotent execution if run cleanly)
DROP TRIGGER IF EXISTS on_auth_user_created_billing ON auth.users;
CREATE TRIGGER on_auth_user_created_billing
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE PROCEDURE public.handle_new_user_billing();


-- 3. In-Depth Transaction Auditing (For Stripe Reconciliation)
CREATE TABLE IF NOT EXISTS public.billing_transactions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  billing_profile_id UUID NOT NULL REFERENCES public.billing_profiles(id) ON DELETE CASCADE,
  amount DECIMAL NOT NULL,
  transaction_type VARCHAR NOT NULL, -- 'compute_deduction', 'stripe_recharge'
  description TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

ALTER TABLE public.billing_transactions ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Users can view their own transactions"
  ON public.billing_transactions
  FOR SELECT
  USING (auth.uid() IN (SELECT owner_id FROM public.billing_profiles WHERE id = billing_profile_id));
