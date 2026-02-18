/**
 * @file features/auth/SignupPage.tsx
 * @description Sign-up page component.
 * Includes user ID, name, and password form and sign-up API integration.
 */
import { ArrowRight, Lock, User, UserPlus } from 'lucide-react';
import { useState } from 'react';
import { useNavigate } from 'react-router-dom';

import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { useAuth } from './AuthContext';

export function SignupPage() {
  const navigate = useNavigate();
  const { signup } = useAuth(); // Get signup function from AuthContext

  // Form state
  const [userId, setUserId] = useState('');
  const [password, setPassword] = useState('');
  const [name, setName] = useState('');
  const [error, setError] = useState('');
  const [isLoading, setIsLoading] = useState(false);

  // Form submit handler
  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setError('');

    try {
      await signup({ userId, password, name }); // Call sign-up API
      alert('Sign up completed. Please sign in.');
      navigate('/login'); // Navigate to login on success
    } catch (err) {
      if (err instanceof Error) {
        setError(err.message);
      } else {
        setError('Sign up failed.');
      }
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className='min-h-screen w-full flex items-center justify-center relative overflow-hidden bg-gray-50'>
      {/* Background decoration elements */}
      <div className='absolute inset-0 z-0'>
        <div className='absolute top-[-10%] left-[-10%] w-[40%] h-[40%] rounded-full bg-primary/20 blur-[120px] animate-pulse' />
        <div className='absolute bottom-[-10%] right-[-10%] w-[40%] h-[40%] rounded-full bg-indigo-400/20 blur-[120px] animate-pulse delay-1000' />
        <div className="absolute top-[20%] left-[20%] w-[60%] h-[60%] bg-[url('https://images.unsplash.com/photo-1497366216548-37526070297c?auto=format&fit=crop&w=1920&q=80')] bg-cover opacity-[0.03]" />
      </div>

      <div className='relative z-10 w-full max-w-110 px-6 animate-fade-in'>
        {/* Sign-up card */}
        <div className='bg-white/70 backdrop-blur-2xl rounded-3xl shadow-2xl border border-white/50 p-8 md:p-10 ring-1 ring-gray-900/5'>
          {/* Header */}
          <div className='text-center mb-10'>
            <div className='mx-auto w-14 h-14 bg-primary/10 rounded-2xl flex items-center justify-center mb-6 text-primary'>
              <UserPlus className='w-8 h-8' />
            </div>
            <h1 className='text-3xl font-bold tracking-tight text-gray-900'>Sign Up</h1>
            <p className='mt-3 text-gray-500 text-sm font-medium'>Create a new account</p>
          </div>

          {/* Sign-up form */}
          <form onSubmit={handleSubmit} className='space-y-6'>
            <div className='space-y-5'>
              <Input
                label='User ID'
                placeholder='Enter the user ID you want to use'
                value={userId}
                onChange={(e) => setUserId(e.target.value)}
                required
                leftIcon={<User size={18} />}
                className='bg-white/50'
              />
              <Input
                label='Name'
                placeholder='Enter your name'
                value={name}
                onChange={(e) => setName(e.target.value)}
                required
                leftIcon={<User size={18} />}
                className='bg-white/50'
              />
              <Input
                label='Password'
                type='password'
                placeholder='Enter your password'
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                leftIcon={<Lock size={18} />}
                className='bg-white/50'
              />
            </div>

            {/* Error message */}
            {error && (
              <div className='p-3 rounded-xl bg-red-50 border border-red-100 flex items-center gap-3 text-sm text-red-600 animate-slide-up'>
                <svg className='w-5 h-5 shrink-0' fill='none' viewBox='0 0 24 24' stroke='currentColor'>
                  <path
                    strokeLinecap='round'
                    strokeLinejoin='round'
                    strokeWidth={2}
                    d='M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z'
                  />
                </svg>
                {error}
              </div>
            )}

            <Button
              type='submit'
              className='w-full h-14 text-base font-semibold rounded-xl mt-2 group relative overflow-hidden transition-all duration-300 hover:-translate-y-1 hover:shadow-xl'
              isLoading={isLoading}
            >
              <span className='relative z-10 flex items-center justify-center gap-2'>
                Create account
                <ArrowRight size={18} className='group-hover:translate-x-1 transition-transform' />
              </span>
            </Button>
          </form>

          {/* Footer */}
          <div className='mt-8 pt-6 border-t border-gray-100 text-center'>
            <p className='text-sm text-gray-500'>
              Already have an account?{' '}
              <button
                type='button'
                className='font-semibold text-primary hover:text-primary-hover transition-colors cursor-pointer hover:underline'
                onClick={() => navigate('/login')}
              >
                Sign in
              </button>
            </p>
          </div>
        </div>

        <p className='text-center mt-8 text-xs text-gray-400 font-medium'>© 2024 Your Company. All rights reserved.</p>
      </div>
    </div>
  );
}
