import { useState, useRef, useEffect, type FC, type MouseEvent } from 'react';

interface SearchableDropdownProps {
    options?: string[];
    value: string | string[];
    onChange: (value: string | string[]) => void;
    placeholder?: string;
    allowFreeText?: boolean;
    multiple?: boolean;
}

const SearchableDropdown: FC<SearchableDropdownProps> = ({
    options =[], value, onChange, placeholder = 'Search...', allowFreeText = false, multiple = false
}) => {
    const [open, setOpen] = useState(false);
    const[search, setSearch] = useState('');
    const containerRef = useRef<HTMLDivElement>(null);
    const inputRef = useRef<HTMLInputElement>(null);

    const selectedItems = multiple && Array.isArray(value) ? value :[];
    
    const filtered = options.filter(opt => {
        if (multiple) {
            if (selectedItems.includes(opt)) return false;
            return opt.toLowerCase().includes(search.toLowerCase());
        }
        const searchTerm = search !== '' ? search : (typeof value === 'string' ? value : '');
        return opt.toLowerCase().includes(searchTerm.toLowerCase());
    });

    const handleSelect = (opt: string) => {
        if (multiple) {
            onChange([...selectedItems, opt]);
            setSearch('');
        } else {
            onChange(opt);
            setSearch('');
            setOpen(false);
        }
        inputRef.current?.focus();
    };

    const handleRemove = (e: MouseEvent, opt: string) => {
        e.stopPropagation();
        onChange(selectedItems.filter(i => i !== opt));
    };

    useEffect(() => {
        const handler = (e: globalThis.MouseEvent) => {
            if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
                setOpen(false);
            }
        };
        document.addEventListener('mousedown', handler);
        return () => document.removeEventListener('mousedown', handler);
    },[]);

    return (
        <div className="sd-container" ref={containerRef} onClick={() => { setOpen(true); inputRef.current?.focus(); }}>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px', padding: '5px 8px', border: '1px solid var(--border)', borderRadius: '6px', background: 'var(--bg-surface)' }}>
                {multiple && selectedItems.map(opt => (
                    <span key={opt} style={{ background: 'var(--accent-muted)', color: 'var(--accent)', padding: '2px 8px', borderRadius: '12px', fontSize: '11px', display: 'flex', alignItems: 'center', gap: '6px' }}>
                        {opt} <span style={{ cursor: 'pointer', fontWeight: 'bold' }} onClick={(e) => handleRemove(e, opt)}>×</span>
                    </span>
                ))}
                <input
                    ref={inputRef}
                    style={{ border: 'none', outline: 'none', background: 'transparent', flex: 1, minWidth: '80px', fontSize: '12px', fontFamily: 'var(--font-mono)', color: 'var(--text-primary)' }}
                    value={multiple ? search : (open ? search : (typeof value === 'string' ? value : ''))}
                    onChange={(e) => {
                        const val = e.target.value;
                        setSearch(val);
                        if (!multiple && allowFreeText) onChange(val);
                    }}
                    onFocus={() => { setOpen(true); if(!multiple) setSearch(typeof value === 'string' ? value : ''); }}
                    placeholder={multiple && selectedItems.length > 0 ? '' : placeholder}
                />
            </div>
            {open && filtered.length > 0 && (
                <div className="sd-dropdown" style={{ position: 'absolute', top: '100%', left: 0, right: 0, background: 'var(--bg-surface)', border: '1px solid var(--border)', zIndex: 100, maxHeight: '200px', overflowY: 'auto', borderRadius: '6px', marginTop: '4px', boxShadow: 'var(--shadow-lg)' }}>
                    {filtered.map(opt => (
                        <div key={opt} className="sd-option" style={{ padding: '6px 10px', fontSize: '12px', cursor: 'pointer', fontFamily: 'var(--font-mono)' }} onMouseDown={(e) => { e.preventDefault(); handleSelect(opt); }}>
                            {opt}
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
};
export default SearchableDropdown;
