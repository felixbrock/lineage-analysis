export const biToolTypes = ['Mode', 'Tableau', 'Metabase'] as const;
export type BiToolType = typeof biToolTypes[number];

export const parseBiToolType = (
  biToolType: string
): BiToolType => {
  const identifiedElement = biToolTypes.find(
    (element) => element.toLowerCase() === biToolType.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};