import * as React from 'react';

import { cn } from '../../lib/utils';

interface TextareaProps extends React.TextareaHTMLAttributes<HTMLTextAreaElement> {
  label?: string;
  error?: string;
}

export const Textarea = React.forwardRef<HTMLTextAreaElement, TextareaProps>(
  ({ className, label, error, ...props }, ref) => {
    return (
      <div className='w-full space-y-2'>
        {label && <label className='text-sm font-semibold text-gray-700'>{label}</label>}
        <textarea
          className={cn(
            'flex w-full rounded-xl border border-gray-200 bg-gray-50/50 px-3 py-2 text-sm text-gray-900 placeholder:text-gray-400 focus:bg-white focus:outline-none focus:ring-2 focus:ring-primary/20 focus:border-primary transition-all duration-200',
            error && 'border-red-500 focus:ring-red-500 bg-red-50/10',
            className,
          )}
          ref={ref}
          {...props}
        />
        {error && <p className='text-sm text-red-500 font-medium animate-fade-in'>{error}</p>}
      </div>
    );
  },
);
Textarea.displayName = 'Textarea';
